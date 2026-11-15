// Package ingest receives stamped batches from proxies and writes them to
// the store in few, large inserts. ClickHouse is fast with big inserts and
// slow with many small ones, so batching here is the point of this role.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

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
	QueueLimit int           // events held in memory, the hard bound
	FlushRows  int           // flush as soon as this many are waiting
	FlushEvery time.Duration // and at least this often
}

const (
	// maxBody must fit one full batch from a proxy, see proxy.New.
	maxBody       = 16 << 20
	insertTimeout = 10 * time.Second
)

type Server struct {
	cfg   Config
	queue *event.Queue
	kick  chan struct{} // tells the flusher that FlushRows was reached
}

func New(cfg Config) *Server {
	return &Server{cfg: cfg, queue: event.NewQueue(cfg.QueueLimit), kick: make(chan struct{}, 1)}
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
	// The store is slow or down: push back to the proxy, do not grow.
	if !s.queue.TryPush(in.Events) {
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
	go s.flushLoop(ctx)
	srv := &http.Server{Addr: s.cfg.Addr, Handler: s.Handler(), ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	return httpx.Serve(ctx, srv)
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
