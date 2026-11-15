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

func post(s *Server, n int) *httptest.ResponseRecorder {
	body, _ := json.Marshal(event.Batch{Events: make([]event.Event, n)})
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/batch", strings.NewReader(string(body))))
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
