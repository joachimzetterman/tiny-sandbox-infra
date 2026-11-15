package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
)

// gateway429 refuses everything and counts the requests it saw.
func gateway429(t *testing.T) (*httptest.Server, *atomic.Int64) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	return srv, &requests
}

// run drives 30 ticks (3 simulated seconds) of one behaviour.
func run(url, behaviour string) {
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	s := &sandbox{cfg: Config{EventsURL: url, Clock: fake}, events: http.DefaultClient, egress: http.DefaultClient}
	for n := 1; n <= 30; n++ { // starts at 1 to skip the egress call on tick 0
		if n%20 != 0 {
			s.step(context.Background(), n, behaviour)
		}
		fake.Advance(tick)
	}
}

func TestWellBehavedSandboxBacksOffOn429(t *testing.T) {
	srv, requests := gateway429(t)
	run(srv.URL, "normal")
	// Told to wait 2 s twice within 3 s: one request at the start, one after the wait.
	if got := requests.Load(); got != 2 {
		t.Errorf("a polite sandbox sent %d requests in 3 s of 429s, want 2", got)
	}
}

func TestFloodIgnores429(t *testing.T) {
	srv, requests := gateway429(t)
	run(srv.URL, "flood")
	if got := requests.Load(); got != 29 {
		t.Errorf("a flood sent %d requests, want one per tick (29)", got)
	}
}
