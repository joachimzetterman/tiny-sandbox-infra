package guard

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
)

// Sink is where the audit trail and the usage rows end up. ClickHouse in
// the demo, nothing in replay.
type Sink interface {
	InsertActions(ctx context.Context, rows []Decision) error
	InsertUsage(ctx context.Context, rows []Usage) error
}

const (
	maxHeartbeat = 64 << 10        // bytes in one heartbeat
	sweepEvery   = time.Second     // how often expired actions are lifted
	flushEvery   = 2 * time.Second // how often rows go to the store
	flushTimeout = 5 * time.Second
)

func (g *Guard) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/heartbeat", g.handleHeartbeat)
	// For ingest: whose events to drop.
	mux.HandleFunc("GET /v1/deny", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, control.DenyList{Sandboxes: g.Denied()})
	})
	// For the demo timeline: decisions made after number ?after=.
	mux.HandleFunc("GET /v1/decisions", func(w http.ResponseWriter, r *http.Request) {
		after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		httpx.WriteJSON(w, http.StatusOK, g.Since(after))
	})
	return mux
}

func (g *Guard) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb control.Heartbeat
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxHeartbeat)).Decode(&hb); err != nil || hb.SandboxID == "" {
		httpx.Refuse(w, http.StatusBadRequest, event.Malformed, 0)
		return
	}
	policy, err := g.Heartbeat(hb)
	if err != nil {
		// The proxy keeps its static limits and its last policy.
		slog.Error("heartbeat refused", "tenant_id", hb.TenantID, "sandbox_id", hb.SandboxID, "err", err)
		httpx.Refuse(w, http.StatusServiceUnavailable, event.GuardFull, 0)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, policy)
}

// Run serves HTTP, lifts expired actions and writes to the store until ctx
// is cancelled.
func (g *Guard) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	looping := make(chan struct{})
	go func() { g.loop(ctx); close(looping) }()
	srv := &http.Server{Addr: g.cfg.Addr, Handler: g.Handler(), ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
	err := httpx.Serve(ctx, srv)
	cancel()
	<-looping
	last, stop := context.WithTimeout(context.Background(), flushTimeout)
	defer stop()
	g.Flush(last) // the audit trail of the last seconds
	return err
}

func (g *Guard) loop(ctx context.Context) {
	sweep, flush := time.NewTicker(sweepEvery), time.NewTicker(flushEvery)
	defer sweep.Stop()
	defer flush.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweep.C:
			g.Sweep()
		case <-flush.C:
			g.Flush(ctx)
		}
	}
}

// Flush writes the waiting rows. Rows leave memory only after the store
// took them. If the store is down they wait, up to maxPending, and the
// guard keeps deciding: containment matters more than the record of it,
// and every decision is in the log as well.
func (g *Guard) Flush(ctx context.Context) {
	if g.cfg.Sink == nil {
		return
	}
	g.mu.Lock()
	actions, usage, lost := g.actions, g.usage, g.lost
	g.lost = 0
	g.mu.Unlock()
	if lost > 0 {
		slog.Error("the store fell behind, rows were dropped", "rows", lost)
	}
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	if err := g.cfg.Sink.InsertActions(ctx, actions); err != nil {
		slog.Error("writing the audit trail failed, will retry", "err", err, "rows", len(actions))
		actions = nil
	}
	if err := g.cfg.Sink.InsertUsage(ctx, usage); err != nil {
		slog.Error("writing usage failed, will retry", "err", err, "rows", len(usage))
		usage = nil
	}
	// Only this function removes rows, so what was written is still at the front.
	g.mu.Lock()
	g.actions, g.usage = g.actions[len(actions):], g.usage[len(usage):]
	g.mu.Unlock()
}
