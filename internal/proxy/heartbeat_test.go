package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
)

// fakeGuard records the last heartbeat and answers with a policy the test sets.
type fakeGuard struct {
	*httptest.Server
	mu     sync.Mutex
	last   control.Heartbeat
	policy control.Policy
}

func newFakeGuard(t *testing.T) *fakeGuard {
	t.Helper()
	f := &fakeGuard{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewDecoder(r.Body).Decode(&f.last)
		_ = json.NewEncoder(w).Encode(f.policy)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeGuard) setPolicy(p control.Policy) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policy = p
}

func (f *fakeGuard) heartbeat() control.Heartbeat {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

// beating starts a proxy that sends heartbeats to guard every 10 ms.
func beating(t *testing.T, guard *fakeGuard) *Proxy {
	t.Helper()
	p, _ := newProxy(func(c *Config) { c.GuardURL = guard.URL; c.Heartbeat = 10 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.heartbeatLoop(ctx)
	return p
}

func TestHeartbeatCarriesTheCountersAndBringsBackThePolicy(t *testing.T) {
	guard := newFakeGuard(t)
	p := beating(t, guard)
	post(p, events(3))
	eventually(t, "the guard to see the counters", func() bool {
		hb := guard.heartbeat()
		return hb.TenantID == "t-real" && hb.SandboxID == "s-real" && hb.Counters.EventsAccepted == 3
	})

	cut := control.Policy{Throttle: true, EventRate: 0, BlockEgress: true}
	guard.setPolicy(cut)
	eventually(t, "the proxy to apply the policy", func() bool { return p.Policy() == cut })
	// The cut-off: no event passes, and the producer is told to stay away.
	rec := post(p, events(1))
	if rec.Code != http.StatusTooManyRequests || refusal(t, rec).Reason != event.RateLimited || rec.Header().Get("Retry-After") != "60" {
		t.Errorf("a cut-off sandbox got %d %s", rec.Code, rec.Body)
	}
	// The proxy tells the guard what it enforces, for a guard that restarts.
	eventually(t, "the proxy to report what it enforces", func() bool { return guard.heartbeat().Applied == cut })
}

func TestGuardDownKeepsTheLastPolicyAndTheStaticLimits(t *testing.T) {
	guard := newFakeGuard(t)
	p := beating(t, guard)
	held := control.Policy{Throttle: true, EventRate: 0, BlockEgress: true}
	guard.setPolicy(held)
	eventually(t, "the proxy to apply the policy", func() bool { return p.Policy() == held })

	guard.Close()
	time.Sleep(100 * time.Millisecond) // ten heartbeats into the void
	if p.Policy() != held {
		t.Errorf("the policy loosened while the guard was down: %+v", p.Policy())
	}
	if rec := post(p, events(1)); rec.Code != http.StatusTooManyRequests {
		t.Errorf("a cut-off sandbox got %d while the guard was down", rec.Code)
	}

	// And a proxy that never reached a guard still has its static limits.
	alone, _ := newProxy(func(c *Config) { c.GuardURL = guard.URL; c.Heartbeat = 10 * time.Millisecond })
	if _, err := alone.beat(context.Background()); err == nil {
		t.Fatal("a heartbeat to a closed guard succeeded")
	}
	if rec := post(alone, events(21)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("batch over the static burst got %d", rec.Code)
	}
}

func TestThrottleLowersTheRateAndTakesAwaySavedTokens(t *testing.T) {
	p, clk := newProxy(nil) // 10 events/s, burst 20, and a full bucket
	p.apply(control.Policy{Throttle: true, EventRate: 2})
	// The 20 saved tokens are gone: one second's worth at the new rate is left.
	if rec := post(p, events(3)); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("3 events right after a throttle to 2/s got %d", rec.Code)
	}
	if rec := post(p, events(2)); rec.Code != http.StatusOK {
		t.Fatalf("2 events got %d", rec.Code)
	}
	clk.Advance(time.Second)
	if rec := post(p, events(3)); rec.Code != http.StatusTooManyRequests {
		t.Errorf("the bucket refilled faster than 2/s")
	}

	// A guard can only lower the static rate, whatever it sends.
	p.apply(control.Policy{Throttle: true, EventRate: 1000})
	clk.Advance(time.Hour)
	post(p, events(20)) // empty the bucket
	clk.Advance(time.Second)
	if rec := post(p, events(11)); rec.Code != http.StatusTooManyRequests {
		t.Errorf("a throttle above the static rate raised the limit")
	}

	// Lifting the throttle brings the static rate back.
	p.apply(control.Policy{Throttle: true, EventRate: 0})
	p.apply(control.Policy{})
	clk.Advance(time.Second)
	if rec := post(p, events(10)); rec.Code != http.StatusOK {
		t.Errorf("10 events a second after the throttle was lifted got %d", rec.Code)
	}
}

func TestBlockedEgressStopsOutboundCallsAndNothingElse(t *testing.T) {
	upstream, hits := countingUpstream(t)
	p, client := egressSetup(t, upstream, testSites)
	p.apply(control.Policy{BlockEgress: true})

	for target, want := range map[string]event.Reason{
		"http://allowed.test/": event.EgressBlocked,
		"http://paste.test/":   event.EgressBlocked,
		"http://evil.test/":    event.SiteBad, // still counted as what it is, so the rule keeps seeing it
	} {
		resp, err := client.Get(target)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var r struct{ Reason event.Reason }
		_ = json.Unmarshal(body, &r)
		if resp.StatusCode != http.StatusForbidden || r.Reason != want {
			t.Errorf("%s: got %d %s, want 403 %s", target, resp.StatusCode, body, want)
		}
	}
	c := p.Counters()
	if hits.Load() != 0 || c.Blocked[event.EgressBlocked] != 2 || c.Blocked[event.SiteBad] != 1 || len(c.EgressBytes) != 0 {
		t.Errorf("upstream hits %d, counters %+v", hits.Load(), c)
	}
	// Events are a separate control: blocking egress does not touch them.
	p.cfg.PeerIP = testPeer
	if rec := post(p, events(3)); rec.Code != http.StatusOK {
		t.Errorf("events got %d while egress was blocked", rec.Code)
	}
}
