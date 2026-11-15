// Package ingest receives stamped batches from proxies and writes them to
// the store in few, large inserts. ClickHouse is fast with big inserts and
// slow with many small ones, so batching here is the point of this role.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
)

// Sink is where batches end up. ClickHouse in production, a fake in tests.
type Sink interface {
	InsertEvents(ctx context.Context, evs []event.Event) error
}

type Config struct {
	Addr       string
	Sink       Sink
	QueueLimit int           // events held in memory
	QueueBytes int           // and their size, whichever is hit first. Zero means 64 MiB.
	FlushRows  int           // flush as soon as this many are waiting
	FlushEvery time.Duration // and at least this often
	GuardURL   string        // where the deny list comes from. Empty means no guard.
	DenyEvery  time.Duration // how often it is fetched. Zero means every second.
}

const (
	// maxBody must fit one full batch from a proxy, see proxy.New.
	maxBody       = 16 << 20
	insertTimeout = 10 * time.Second
	drainTimeout  = 5 * time.Second // the last flush, inside the 10 s that docker stop allows
	denyTimeout   = 800 * time.Millisecond
)

type Server struct {
	cfg   Config
	queue *event.Queue
	kick  chan struct{} // tells the flusher that FlushRows was reached

	mu     sync.Mutex      // guards denied
	denied map[string]bool // sandboxes whose events are dropped
}

func New(cfg Config) *Server {
	if cfg.QueueBytes == 0 {
		cfg.QueueBytes = 64 << 20
	}
	if cfg.DenyEvery == 0 {
		cfg.DenyEvery = time.Second
	}
	return &Server{cfg: cfg, queue: event.NewQueue(cfg.QueueLimit, cfg.QueueBytes), kick: make(chan struct{}, 1)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/batch", s.handleBatch)
	return mux
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var in event.Batch
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&in); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			httpx.Refuse(w, http.StatusRequestEntityTooLarge, event.BodyTooLarge, 0)
			return
		}
		httpx.Refuse(w, http.StatusBadRequest, event.Malformed, 0)
		return
	}
	// The queue is shared by every tenant and a failed insert is retried,
	// so an event the store can never take must not get in.
	for _, e := range in.Events {
		if !event.Stamped(e) {
			slog.Warn("refused a batch with an event no proxy would send", "events", len(in.Events))
			httpx.Refuse(w, http.StatusBadRequest, event.Malformed, 0)
			return
		}
	}
	// Enforcement must not depend on one proxy behaving. Events from a
	// sandbox the guard killed or cut off are dropped here as well. The
	// proxy still gets its 204, so it does not send them again.
	s.mu.Lock()
	denied := s.denied
	s.mu.Unlock()
	kept := in.Events[:0] // Go idiom: filters in place, reusing the same memory
	var last event.Event  // the last one dropped
	for _, e := range in.Events {
		if denied[e.SandboxID] {
			last = e
			continue
		}
		kept = append(kept, e)
	}
	if n := len(in.Events) - len(kept); n > 0 {
		// A batch comes from one proxy, so every dropped event names the same sandbox.
		slog.Info("dropped events of a denied sandbox", "tenant_id", last.TenantID, "sandbox_id", last.SandboxID, "events", n)
	}
	// The store is slow or down: push back to the proxy, do not grow.
	if !s.queue.TryPush(kept) {
		httpx.Refuse(w, http.StatusTooManyRequests, event.BufferFull, time.Second)
		return
	}
	if s.queue.Len() >= s.cfg.FlushRows {
		// Go idiom: select with default is a non-blocking send. If a kick
		// is already waiting, one more adds nothing.
		select {
		case s.kick <- struct{}{}:
		default:
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// Run serves HTTP and flushes until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	flushing := make(chan struct{})
	go func() { s.flushLoop(ctx); close(flushing) }()
	if s.cfg.GuardURL != "" {
		go s.denyLoop(ctx)
	}
	srv := &http.Server{Addr: s.cfg.Addr, Handler: s.Handler(), ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	err := httpx.Serve(ctx, srv)
	// The listener is closed, so nothing new can arrive. Proxies already
	// got a 204 for what is still queued, so write it before the process ends.
	cancel()
	<-flushing
	last, stop := context.WithTimeout(context.Background(), drainTimeout)
	defer stop()
	s.flush(last)
	return err
}

// flushLoop writes on rows or on time, whichever comes first.
func (s *Server) flushLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.FlushEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-s.kick:
		}
		s.flush(ctx)
	}
}

// flush writes everything waiting as one insert. Events leave the queue
// only after the insert worked, so a failed insert is retried next time.
func (s *Server) flush(ctx context.Context) {
	batch := s.queue.Peek(s.cfg.QueueLimit)
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, insertTimeout)
	defer cancel()
	if err := s.cfg.Sink.InsertEvents(ctx, batch); err != nil {
		slog.Error("insert failed, will retry", "err", err, "rows", len(batch))
		return
	}
	s.queue.Drop(len(batch))
	slog.Info("flushed", "rows", len(batch))
}

// denyLoop keeps the deny list fresh. If the guard cannot be reached the
// last list stays in force: a guard that is down never loosens anything.
func (s *Server) denyLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.DenyEvery)
	defer tick.Stop()
	down := false // used to log once per outage
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		denied, err := s.fetchDenied(ctx)
		if err != nil {
			if !down {
				slog.Warn("guard unreachable, keeping the last deny list", "err", err)
			}
			down = true
			continue
		}
		down = false
		s.mu.Lock()
		s.denied = denied
		s.mu.Unlock()
	}
}

func (s *Server) fetchDenied(ctx context.Context) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, denyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.GuardURL+"/v1/deny", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var list control.DenyList
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("guard replied " + resp.Status)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&list); err != nil {
		return nil, err
	}
	denied := map[string]bool{}
	for _, id := range list.Sandboxes {
		denied[id] = true
	}
	return denied, nil
}
