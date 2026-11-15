package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
)

// fakeSink records every insert and can be told to fail.
type fakeSink struct {
	mu      sync.Mutex
	inserts [][]event.Event
	fail    bool
}

func (f *fakeSink) InsertEvents(_ context.Context, evs []event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("store is down")
	}
	f.inserts = append(f.inserts, evs)
	return nil
}

func (f *fakeSink) sizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []int
	for _, in := range f.inserts {
		out = append(out, len(in))
	}
	return out
}

func (f *fakeSink) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

func start(t *testing.T, cfg Config) *Server {
	t.Helper()
	s := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.flushLoop(ctx)
	return s
}

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// post sends n events as a proxy would: stamped.
func post(s *Server, n int) *httptest.ResponseRecorder {
	batch := event.Batch{Events: make([]event.Event, n)}
	for i := range batch.Events {
		batch.Events[i] = event.Event{Type: "log", Payload: "hello"}
		event.Stamp(&batch.Events[i], "t1", "s1", t0)
	}
	body, _ := json.Marshal(batch)
	return postBody(s, string(body))
}

func postBody(s *Server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/batch", strings.NewReader(body)))
	return rec
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

func TestSmallBatchesBecomeOneInsertOnTime(t *testing.T) {
	sink := &fakeSink{}
	s := start(t, Config{Sink: sink, QueueLimit: 100, FlushRows: 100, FlushEvery: 50 * time.Millisecond})
	for range 3 {
		if rec := post(s, 2); rec.Code != http.StatusNoContent {
			t.Fatalf("status %d", rec.Code)
		}
	}
	eventually(t, "time flush", func() bool { return len(sink.sizes()) > 0 })
	if got := sink.sizes(); len(got) != 1 || got[0] != 6 {
		t.Errorf("inserts = %v, want one insert of 6 rows", got)
	}
}

func TestFlushOnRowsDoesNotWaitForTheTimer(t *testing.T) {
	sink := &fakeSink{}
	s := start(t, Config{Sink: sink, QueueLimit: 100, FlushRows: 5, FlushEvery: time.Hour})
	post(s, 4)
	time.Sleep(30 * time.Millisecond)
	if len(sink.sizes()) != 0 {
		t.Fatal("flushed before the row threshold")
	}
	post(s, 1)
	eventually(t, "row flush", func() bool { return len(sink.sizes()) == 1 })
}

func TestSlowStoreGives429AndTheQueueNeverGrows(t *testing.T) {
	sink := &fakeSink{fail: true}
	s := start(t, Config{Sink: sink, QueueLimit: 10, FlushRows: 5, FlushEvery: 5 * time.Millisecond})
	if rec := post(s, 10); rec.Code != http.StatusNoContent {
		t.Fatalf("first batch refused: %d", rec.Code)
	}
	for range 5 {
		rec := post(s, 1)
		if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" || !strings.Contains(rec.Body.String(), string(event.BufferFull)) {
			t.Fatalf("got %d %s, want 429 buffer_full with Retry-After", rec.Code, rec.Body)
		}
		if s.queue.Len() != 10 {
			t.Fatalf("queue length %d, limit is 10", s.queue.Len())
		}
	}
	// Nothing accepted is lost: it is written once the store is back.
	sink.setFail(false)
	eventually(t, "drain after recovery", func() bool { return s.queue.Len() == 0 })
	if got := sink.sizes(); len(got) != 1 || got[0] != 10 {
		t.Errorf("inserts = %v, want one insert of 10 rows", got)
	}
}

func TestMalformedBatchIsRefusedWithAReason(t *testing.T) {
	s := New(Config{QueueLimit: 10, FlushRows: 5})
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/batch", strings.NewReader("{")))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), string(event.Malformed)) {
		t.Errorf("got %d %s", rec.Code, rec.Body)
	}
}

func TestEventTheStoreCannotTakeNeverEntersTheQueue(t *testing.T) {
	s := New(Config{QueueLimit: 10, FlushRows: 5})
	for _, body := range []string{
		`{"events":[{}]}`,
		`{"events":[{"type":"log","tenant_id":"t1","sandbox_id":"s1","event_time":"2026-01-02T03:04:05Z","event_id":"not-a-uuid"}]}`,
	} {
		rec := postBody(s, body)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), string(event.Malformed)) {
			t.Errorf("%s: got %d %s, want 400 malformed", body, rec.Code, rec.Body)
		}
	}
	// The shared queue is untouched, so other tenants are not held up.
	if s.queue.Len() != 0 {
		t.Fatalf("%d poisoned events were queued", s.queue.Len())
	}
	if rec := post(s, 10); rec.Code != http.StatusNoContent {
		t.Errorf("a good batch after the bad ones: %d %s", rec.Code, rec.Body)
	}
}

func TestShutdownWritesWhatIsStillQueued(t *testing.T) {
	sink := &fakeSink{}
	// Neither flush trigger can fire, so only the shutdown can write these.
	s := New(Config{Addr: "127.0.0.1:0", Sink: sink, QueueLimit: 100, FlushRows: 100, FlushEvery: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- s.Run(ctx) }()
	if rec := post(s, 3); rec.Code != http.StatusNoContent {
		t.Fatalf("status %d", rec.Code)
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("ingest stopped with an error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if got := sink.sizes(); len(got) != 1 || got[0] != 3 {
		t.Errorf("inserts = %v, want the 3 acknowledged rows written on the way out", got)
	}
}
