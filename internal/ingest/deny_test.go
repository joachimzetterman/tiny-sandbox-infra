package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
)

// postFrom sends n stamped events from one sandbox.
func postFrom(s *Server, sandboxID string, n int) *httptest.ResponseRecorder {
	batch := event.Batch{Events: make([]event.Event, n)}
	for i := range batch.Events {
		batch.Events[i] = event.Event{Type: "log", Payload: "hello"}
		event.Stamp(&batch.Events[i], "t1", sandboxID, t0)
	}
	body, _ := json.Marshal(batch)
	return postBody(s, string(body))
}

func TestEventsOfADeniedSandboxAreDroppedAlsoWhenTheGuardGoesDown(t *testing.T) {
	var fetched atomic.Int64
	guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetched.Add(1)
		_ = json.NewEncoder(w).Encode(control.DenyList{Sandboxes: []string{"killed"}})
	}))
	defer guard.Close()
	sink := &fakeSink{}
	s := start(t, Config{Sink: sink, QueueLimit: 100, FlushRows: 100, FlushEvery: 10 * time.Millisecond, GuardURL: guard.URL, DenyEvery: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.denyLoop(ctx)
	eventually(t, "the deny list to arrive", func() bool { return fetched.Load() > 0 })
	time.Sleep(20 * time.Millisecond)

	stored := func() int {
		n := 0
		for _, size := range sink.sizes() {
			n += size
		}
		return n
	}
	// The proxy gets a 204, so it does not send them again. Nothing is stored.
	if rec := postFrom(s, "killed", 5); rec.Code != http.StatusNoContent {
		t.Fatalf("a denied sandbox's batch got %d", rec.Code)
	}
	postFrom(s, "fine", 3)
	eventually(t, "the other sandbox's events to be stored", func() bool { return stored() == 3 })

	// Guard gone: the last list stays in force.
	guard.Close()
	time.Sleep(50 * time.Millisecond)
	postFrom(s, "killed", 5)
	postFrom(s, "fine", 2)
	eventually(t, "the other sandbox's events to be stored", func() bool { return stored() == 5 })
	time.Sleep(30 * time.Millisecond)
	if stored() != 5 {
		t.Errorf("stored %d events, want 5: the deny list was forgotten when the guard went down", stored())
	}
}
