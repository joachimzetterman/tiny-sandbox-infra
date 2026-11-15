package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// testPeer is the address httptest.NewRequest uses as the caller.
const testPeer = "192.0.2.1"

func newProxy(change func(*Config)) (*Proxy, *clock.Fake) {
	fake := clock.NewFake(t0)
	cfg := Config{
		TenantID: "t-real", SandboxID: "s-real", PeerIP: testPeer, Rate: 10, Burst: 20, Clock: fake,
		Sites: scenario.Sites{Trusted: []string{"allowed.test"}, Watched: []string{"paste.test"}, Bad: []string{"evil.test"}},
	}
	if change != nil {
		change(&cfg)
	}
	return New(cfg), fake
}

// events builds a request body with n good events.
func events(n int) string {
	batch := event.Batch{Events: make([]event.Event, n)}
	for i := range batch.Events {
		batch.Events[i] = event.Event{Type: "log", EventTime: t0, Payload: "hello"}
	}
	body, _ := json.Marshal(batch)
	return string(body)
}

func post(p *Proxy, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	p.EventsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))
	return rec
}

func refusal(t *testing.T, rec *httptest.ResponseRecorder) httpx.Refusal {
	t.Helper()
	var r httpx.Refusal
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("reply is not a refusal: %s", rec.Body)
	}
	return r
}

// fakeIngest records what the proxy forwards and can be told to fail.
type fakeIngest struct {
	*httptest.Server
	mu     sync.Mutex
	got    []event.Event
	status int
}

func newFakeIngest() *fakeIngest {
	f := &fakeIngest{status: http.StatusNoContent}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var in event.Batch
		_ = json.NewDecoder(r.Body).Decode(&in)
		if f.status/100 == 2 {
			f.got = append(f.got, in.Events...)
		}
		w.WriteHeader(f.status)
	}))
	return f
}

func (f *fakeIngest) received() []event.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]event.Event(nil), f.got...)
}

func (f *fakeIngest) setStatus(s int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = s
}

// eventually polls, because the forwarder runs on its own goroutine.
func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

func TestSpoofedTenantIsOverwrittenAllTheWayToIngest(t *testing.T) {
	ingest := newFakeIngest()
	defer ingest.Close()
	p, _ := newProxy(func(c *Config) { c.IngestURL = ingest.URL; c.ForwardEvery = 5 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.forwardLoop(ctx)

	rec := post(p, `{"events":[{"type":"log","tenant_id":"victim","sandbox_id":"not-me","event_id":"mine","payload":"x"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	eventually(t, "event reaches ingest", func() bool { return len(ingest.received()) == 1 })
	got := ingest.received()[0]
	if got.TenantID != "t-real" || got.SandboxID != "s-real" || got.EventID == "mine" {
		t.Errorf("sandbox-supplied identity survived: %+v", got)
	}
}

func TestBadEventsAreRejectedWithReasonsAndCounted(t *testing.T) {
	p, _ := newProxy(nil)
	big := strings.Repeat("x", event.MaxPayloadBytes+1)
	rec := post(p, `{"events":[{"type":"log","payload":"ok"},{"type":"nope"},{"type":"log","payload":"`+big+`"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var reply Accepted
	_ = json.Unmarshal(rec.Body.Bytes(), &reply)
	want := []RejectedRow{{Index: 1, Reason: event.UnknownType}, {Index: 2, Reason: event.Oversized}}
	if reply.Accepted != 1 || fmt.Sprint(reply.Rejected) != fmt.Sprint(want) {
		t.Errorf("got %+v, want 1 accepted and %v", reply, want)
	}
	c := p.Counters()
	if c.EventsAccepted != 1 || c.Rejects[event.UnknownType] != 1 || c.Rejects[event.Oversized] != 1 {
		t.Errorf("counters wrong: %+v", c)
	}
}

func TestWholeRequestRefusalsCarryReasons(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		status int
		reason event.Reason
	}{
		{"not JSON", `{"events": [`, http.StatusBadRequest, event.Malformed},
		{"body over the byte limit", `{"events":[{"type":"log","payload":"` + strings.Repeat("x", maxEventBody) + `"}]}`, http.StatusRequestEntityTooLarge, event.BodyTooLarge},
		{"more events than the bucket can ever hold", events(21), http.StatusRequestEntityTooLarge, event.BatchTooLarge},
	}
	for _, c := range cases {
		p, _ := newProxy(nil)
		rec := post(p, c.body)
		if rec.Code != c.status || refusal(t, rec).Reason != c.reason {
			t.Errorf("%s: got %d %s, want %d %s", c.name, rec.Code, rec.Body, c.status, c.reason)
		}
		if p.Counters().Rejects[c.reason] == 0 {
			t.Errorf("%s: refusal was not counted", c.name)
		}
	}
}

func TestRateLimitReturns429WithRetryAfter(t *testing.T) {
	p, fake := newProxy(nil) // 10 events per second, burst 20
	if rec := post(p, events(20)); rec.Code != http.StatusOK {
		t.Fatalf("burst refused: %d %s", rec.Code, rec.Body)
	}
	rec := post(p, events(5))
	if rec.Code != http.StatusTooManyRequests || refusal(t, rec).Reason != event.RateLimited {
		t.Fatalf("got %d %s, want 429 rate_limited", rec.Code, rec.Body)
	}
	// 5 tokens at 10 per second is half a second, rounded up to 1.
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
	if got := p.Counters().Rejects[event.RateLimited]; got != 5 {
		t.Errorf("counted %d rate limited events, want 5", got)
	}
	fake.Advance(time.Second)
	if rec := post(p, events(5)); rec.Code != http.StatusOK {
		t.Errorf("still refused after waiting Retry-After: %d %s", rec.Code, rec.Body)
	}
}

func TestEventTimeIsClampedAtTheGateway(t *testing.T) {
	p, _ := newProxy(nil)
	post(p, `{"events":[{"type":"log","event_time":"1999-01-01T00:00:00Z"},{"type":"log","event_time":"2099-01-01T00:00:00Z"}]}`)
	got := p.queue.Peek(2)
	if !got[0].EventTime.Equal(t0.Add(-time.Hour)) || !got[1].EventTime.Equal(t0.Add(time.Hour)) {
		t.Errorf("not clamped: %s and %s", got[0].EventTime, got[1].EventTime)
	}
}

func TestSlowIngestGives429AndTheBufferNeverGrows(t *testing.T) {
	ingest := newFakeIngest()
	defer ingest.Close()
	ingest.setStatus(http.StatusServiceUnavailable)
	p, fake := newProxy(func(c *Config) {
		c.IngestURL, c.ForwardEvery, c.QueueLimit = ingest.URL, 5*time.Millisecond, 30
		c.Rate, c.Burst = 1000, 1000
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.forwardLoop(ctx)

	accepted, refused := 0, 0
	for range 20 {
		rec := post(p, events(10))
		switch rec.Code {
		case http.StatusOK:
			accepted += 10
		case http.StatusTooManyRequests:
			refused++
			if r := refusal(t, rec); r.Reason != event.BufferFull || rec.Header().Get("Retry-After") == "" {
				t.Fatalf("429 without buffer_full and Retry-After: %s", rec.Body)
			}
		default:
			t.Fatalf("unexpected status %d", rec.Code)
		}
		if n := p.queue.Len(); n > 30 {
			t.Fatalf("buffer grew to %d, limit is 30", n)
		}
		fake.Advance(time.Second)
	}
	if accepted != 30 || refused != 17 {
		t.Errorf("accepted %d and refused %d requests, want 30 and 17", accepted, refused)
	}
	// When ingest recovers, everything that was accepted is delivered.
	ingest.setStatus(http.StatusNoContent)
	eventually(t, "buffer drains after ingest recovers", func() bool { return len(ingest.received()) == 30 && p.queue.Len() == 0 })
}

func TestOnlyTheOwnSandboxIsServed(t *testing.T) {
	p, _ := newProxy(func(c *Config) { c.PeerIP = "10.77.0.2" })
	for _, h := range []http.Handler{p.EventsHandler(), p.EgressHandler()} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(events(1))))
		if rec.Code != http.StatusForbidden || refusal(t, rec).Reason != event.ForbiddenPeer {
			t.Errorf("stranger was served: %d %s", rec.Code, rec.Body)
		}
	}
	if p.queue.Len() != 0 {
		t.Error("a stranger's event was buffered")
	}
}

func TestBucketRefillsAtRateUpToBurst(t *testing.T) {
	fake := clock.NewFake(t0)
	b := newBucket(fake, 10, 20)
	if ok, _ := b.take(20); !ok {
		t.Fatal("a full bucket refused its burst")
	}
	if ok, wait := b.take(1); ok || wait != 100*time.Millisecond {
		t.Errorf("empty bucket: ok=%v wait=%s, want refusal and 100ms", ok, wait)
	}
	fake.Advance(500 * time.Millisecond) // 5 tokens
	if ok, _ := b.take(5); !ok {
		t.Error("5 tokens after half a second were refused")
	}
	fake.Advance(time.Hour) // refill stops at burst
	if ok, _ := b.take(21); ok {
		t.Error("bucket holds more than its burst")
	}
	zero := newBucket(fake, 0, 0)
	if ok, wait := zero.take(1); ok || wait != maxRetryAfter {
		t.Errorf("zero rate: ok=%v wait=%s, want refusal and the cap", ok, wait)
	}
}
