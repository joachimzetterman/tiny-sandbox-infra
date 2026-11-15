package guard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

var start = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler)) // the guard logs every decision
	os.Exit(m.Run())
}

// second is what one sandbox does in one second.
type second struct {
	accepted, rejected, bufferFull int // events
	watched                        int // bytes to a watched site
	badCalls                       int // calls to a bad site
}

// harness drives a guard with made-up traffic and a clock that only moves
// when the test says so. One step is one second: every sandbox sends one
// heartbeat, in the order it was first seen.
type harness struct {
	t       *testing.T
	g       *Guard
	clk     *clock.Fake
	runtime *Nothing
	tenant  map[string]string // sandbox to tenant
	order   []string
	totals  map[string]*control.Counters
	policy  map[string]control.Policy // the last reply to each sandbox
	silent  map[string]bool           // sandboxes that stopped sending heartbeats
	now     int                       // seconds since the start
}

func newHarness(t *testing.T, rules string, tiers map[string]string) *harness {
	t.Helper()
	rs, err := ParseRules([]byte(rules))
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(start)
	runtime := &Nothing{}
	return &harness{
		t: t, clk: clk, runtime: runtime,
		g:      New(Config{Rules: rs, Tiers: tiers, Runtime: runtime, Clock: clk}),
		tenant: map[string]string{}, totals: map[string]*control.Counters{}, policy: map[string]control.Policy{},
		silent: map[string]bool{},
	}
}

// add registers a sandbox. It sends nothing until the next step.
func (h *harness) add(tenant string, ids ...string) {
	for _, id := range ids {
		h.tenant[id] = tenant
		h.order = append(h.order, id)
		h.totals[id] = &control.Counters{Rejects: map[event.Reason]uint64{}, EgressByClass: map[string]uint64{}, Blocked: map[event.Reason]uint64{}}
	}
}

// step moves the clock one second and lets every live sandbox report what
// traffic says it did in that second.
func (h *harness) step(traffic func(id string, sec int) second) {
	h.clk.Advance(time.Second)
	h.now++
	h.g.Sweep()
	for _, id := range h.order {
		if h.silent[id] || slices.Contains(h.runtime.Killed, id) {
			continue // dead sandboxes send no heartbeats
		}
		s, c := traffic(id, h.now), h.totals[id]
		c.EventsAccepted += uint64(s.accepted)
		c.Rejects[event.RateLimited] += uint64(s.rejected)
		c.Rejects[event.BufferFull] += uint64(s.bufferFull)
		c.EgressByClass[scenario.ClassWatched] += uint64(s.watched)
		c.Blocked[event.SiteBad] += uint64(s.badCalls)
		// A copy, as if it had gone over the wire.
		sent := *c
		sent.Rejects, sent.EgressByClass, sent.Blocked = copyOf(c.Rejects), copyOf(c.EgressByClass), copyOf(c.Blocked)
		policy, err := h.g.Heartbeat(control.Heartbeat{TenantID: h.tenant[id], SandboxID: id, Counters: sent})
		if err != nil {
			h.t.Fatal(err)
		}
		h.policy[id] = policy
	}
}

func copyOf[K comparable](m map[K]uint64) map[K]uint64 {
	out := map[K]uint64{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// run steps for a number of seconds with the same traffic function.
func (h *harness) run(seconds int, traffic func(id string, sec int) second) {
	for range seconds {
		h.step(traffic)
	}
}

// decisions returns what the guard decided about one sandbox, as short
// strings like "12s throttle" or "20s kill (blast_radius_cap)".
func (h *harness) decisions(id string) []string {
	var out []string
	for _, d := range h.g.Since(0) {
		if d.SandboxID != id {
			continue
		}
		text := fmt.Sprintf("%ds %s %s", int(d.At.Sub(start).Seconds()), d.Rule, d.Effect)
		if d.Note != "" {
			text += " (" + d.Note + ")"
		}
		out = append(out, text)
	}
	return out
}

func (h *harness) expect(id string, want ...string) {
	h.t.Helper()
	if got := h.decisions(id); !slices.Equal(got, want) {
		h.t.Errorf("decisions about %s:\n got %q\nwant %q", id, got, want)
	}
}

func steady(rate int) func(string, int) second {
	return func(string, int) second { return second{accepted: rate} }
}

const header = "max_kills_per_minute: 3\nrules:\n"

func TestRatesComeFromTheGrowthBetweenTheOldestAndNewestSnapshot(t *testing.T) {
	var w window
	at := func(sec int, accepted, limited, full, watched, bad uint64) {
		w.add(start.Add(time.Duration(sec)*time.Second), control.Counters{
			EventsAccepted: accepted,
			Rejects:        map[event.Reason]uint64{event.RateLimited: limited, event.BufferFull: full},
			EgressByClass:  map[string]uint64{scenario.ClassWatched: watched, scenario.ClassTrusted: 2 * watched},
			Blocked:        map[event.Reason]uint64{event.SiteBad: bad},
		})
	}
	at(0, 1000, 0, 0, 0, 0)
	at(1, 1100, 50, 0, 1000, 1)
	if _, ok := w.rates(); ok {
		t.Fatal("a window that is not full yet must not be judged")
	}
	at(2, 1300, 100, 500, 4000, 3)
	got, _ := w.rates()
	// Over two seconds: 300 accepted, 100 refused for being too fast. The
	// 500 refused because the buffer was full are not the sandbox's doing.
	want := rates{offered: 200, accepted: 150, rejectRatio: 0.25, egressBytes: 6000, watchedBytes: 2000, badSite: 3}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}

	// A heartbeat that arrives late stretches the window, so the rate stays right.
	at(6, 1500, 100, 500, 4000, 3) // the window is now seconds 1, 2 and 6
	if got, _ := w.rates(); got.accepted != 80 {
		t.Errorf("400 events over 5 seconds should be 80/s, got %v", got.accepted)
	}
}

func TestCounterThatWentDownReadsAsNoGrowth(t *testing.T) {
	var w window
	for sec, accepted := range []uint64{5000, 5100, 40} { // the proxy restarted before the third
		w.add(start.Add(time.Duration(sec)*time.Second), control.Counters{EventsAccepted: accepted})
	}
	if got, _ := w.rates(); got.offered != 0 {
		t.Errorf("a counter reset must not look like a huge rate, got %v", got.offered)
	}
}

func TestBaselineFollowsALastingChangeSlowly(t *testing.T) {
	var b baseline
	b.learn(20, start)
	if b.value != 20 {
		t.Fatalf("the first value is taken as is, got %v", b.value)
	}
	for sec := 1; sec <= 5; sec++ {
		b.learn(120, start.Add(time.Duration(sec)*time.Second))
	}
	if b.value < 25 || b.value > 30 {
		t.Errorf("after 5 s at 120 the baseline should have moved a little from 20, got %.1f", b.value)
	}
	for sec := 6; sec <= 60; sec++ {
		b.learn(120, start.Add(time.Duration(sec)*time.Second))
	}
	if b.value < 75 || b.value > 90 {
		t.Errorf("after 60 s it should be about two thirds of the way to 120, got %.1f", b.value)
	}
}

func TestBaselineDoesNotLearnFromBehaviourARuleObjectsTo(t *testing.T) {
	h := newHarness(t, header+`
  - {name: flood, expr: "offered_rate > 5.0 * baseline_rate", for: 2s, action: throttle, rate: 10, mode: enforce, ttl: 10s}`, nil)
	h.add("t1", "s1")
	h.run(30, steady(20))
	h.run(120, steady(2000)) // two minutes of flood
	if b := h.g.sandboxes["s1"].baseline.value; b < 19 || b > 21 {
		t.Errorf("the flood became the new normal: baseline is %.0f, want 20", b)
	}
	// So the rule still fires and the throttle is still in force.
	if !h.policy["s1"].Throttle {
		t.Error("the throttle lapsed while the flood was still going")
	}
	h.expect("s1", "33s flood throttle")
}

func TestRulesAreCheckedWhenTheyAreLoaded(t *testing.T) {
	good := `{name: a, expr: "offered_rate > 100", for: 1s, action: throttle, rate: 5, mode: enforce, ttl: 10s}`
	if _, err := ParseRules([]byte(header + "  - " + good)); err != nil {
		t.Fatalf("a good rule was refused: %v", err)
	}
	bad := map[string]string{
		"a variable that does not exist": `{name: a, expr: "offred_rate > 100.0", action: observe, ttl: 10s}`,
		"an expression that is a number": `{name: a, expr: "offered_rate + 1.0", action: observe, ttl: 10s}`,
		"whole number times a decimal":   `{name: a, expr: "offered_rate > 5 * baseline_rate", action: observe, ttl: 10s}`,
		"an unknown action":              `{name: a, expr: "true", action: quarantine, ttl: 10s}`,
		"an unknown mode":                `{name: a, expr: "true", action: observe, mode: on, ttl: 10s}`,
		"no ttl":                         `{name: a, expr: "true", action: observe}`,
		"a rate on something else":       `{name: a, expr: "true", action: kill, rate: 5, ttl: 10s}`,
		"an unknown key":                 `{name: a, expr: "true", action: observe, ttl: 10s, forr: 1s}`,
		"the same name twice":            good + "\n  - " + good,
	}
	for what, rule := range bad {
		if _, err := ParseRules([]byte(header + "  - " + rule)); err == nil {
			t.Errorf("%s was accepted", what)
		}
	}
	if _, err := ParseRules([]byte("rules:\n  - " + good)); err == nil {
		t.Error("a rule set without a blast radius cap was accepted")
	}
	// A rule that does not say it enforces only watches.
	rs, _ := ParseRules([]byte(header + `  - {name: a, expr: "true", action: kill, ttl: 10s}`))
	if rs.Rules[0].Mode != Shadow {
		t.Errorf("mode defaulted to %q, want shadow", rs.Rules[0].Mode)
	}
}

func TestRuleFiresAfterForAndIsLiftedAfterTTL(t *testing.T) {
	h := newHarness(t, header+`
  - {name: loud, expr: "offered_rate > 100.0", for: 3s, action: throttle, rate: 10, mode: enforce, ttl: 5s}`, nil)
	h.add("t1", "s1")
	h.run(10, steady(20))

	// Two loud seconds, then quiet again: not long enough.
	h.run(2, steady(500))
	h.run(3, steady(20))
	h.expect("s1")

	// Loud from second 15. The window shows it at 16, and three seconds
	// later the rule fires.
	h.run(3, steady(500))
	if h.policy["s1"].Throttle {
		t.Fatal("the rule fired before its condition had held for 3 s")
	}
	h.step(steady(500))
	if want := (control.Policy{Throttle: true, EventRate: 10}); h.policy["s1"] != want {
		t.Fatalf("policy is %+v, want %+v", h.policy["s1"], want)
	}

	// While the rule still fires the action stays, however long.
	h.run(20, steady(500))
	if !h.policy["s1"].Throttle {
		t.Fatal("the action expired although the rule was still firing")
	}

	// Quiet from second 40. The rule fires for the last time at 40, while
	// the window still holds a loud second, and the action is lifted one
	// ttl later.
	h.run(5, steady(20))
	if !h.policy["s1"].Throttle {
		t.Fatal("the action was lifted before its ttl ran out")
	}
	h.run(1, steady(20))
	if h.policy["s1"] != (control.Policy{}) {
		t.Fatalf("the action is still in force after its ttl: %+v", h.policy["s1"])
	}
	h.expect("s1", "19s loud throttle", "45s loud  (lifted)")
}

func TestShadowRuleRecordsWhatItWouldDoAndChangesNothing(t *testing.T) {
	h := newHarness(t, header+`
  - {name: candidate, expr: "offered_rate > 100.0", for: 1s, action: kill, mode: shadow, ttl: 10s}`, nil)
	h.add("t1", "s1")
	h.run(5, steady(20))
	h.run(20, steady(500))
	if h.policy["s1"] != (control.Policy{}) || len(h.runtime.Killed) != 0 {
		t.Errorf("a shadow rule changed something: policy %+v, killed %v", h.policy["s1"], h.runtime.Killed)
	}
	// One row, not one per second.
	h.expect("s1", "7s candidate  (shadow)")
	if d := h.g.Since(0)[0]; d.Action != Kill || d.Inputs["offered_rate"] != 500 {
		t.Errorf("the record must say what would have been done and on what numbers, got %+v", d)
	}
}

func TestSplitFloodIsOnlyVisibleAtTenantLevel(t *testing.T) {
	h := newHarness(t, readRules(t), map[string]string{"split": "free", "single": "free"})
	h.add("split", "a", "b", "c")
	h.add("single", "d")
	h.run(40, steady(20))
	// Every sandbox goes to 90 events/s, under the per-sandbox limit of 100,
	// and stays there. Long enough that a tenant baseline which learned from
	// the flood would have caught up and let the throttle lapse.
	h.run(120, steady(90))
	for _, id := range []string{"a", "b", "c"} {
		if want := (control.Policy{Throttle: true, EventRate: 20}); h.policy[id] != want {
			t.Errorf("%s: policy is %+v, want %+v: the tenant sends 270/s", id, h.policy[id], want)
		}
	}
	// The same sandbox behaviour in a tenant that stays under the tenant limit is fine.
	if h.policy["d"] != (control.Policy{}) {
		t.Errorf("a single sandbox at 90/s was acted on: %+v", h.policy["d"])
	}
	h.expect("d")
}

func TestSandboxThatWentSilentLeavesItsTenantsTotal(t *testing.T) {
	h := newHarness(t, header+`
  - {name: tenant-limit, expr: "tenant_offered_rate > 150.0", for: 1s, action: throttle, rate: 20, mode: enforce, ttl: 5s}`, nil)
	h.add("t1", "a", "b")
	h.run(10, steady(100)) // 200/s together
	if !h.policy["a"].Throttle {
		t.Fatal("the tenant is over its limit and was not throttled")
	}
	// b disappears. Its last window must not count for ever.
	h.silent["b"] = true
	h.run(15, steady(100))
	if h.policy["a"].Throttle {
		t.Error("a is still throttled for a tenant total that includes a sandbox that is gone")
	}
}

func TestPaidTenantIsNeverKilledByAutomation(t *testing.T) {
	h := newHarness(t, header+`
  - {name: bad-site, expr: "bad_site_attempts >= 2.0", for: 1s, action: kill, mode: enforce, ttl: 1m}`,
		map[string]string{"free": "free", "paid": "paid"})
	h.add("free", "f")
	h.add("paid", "p")
	h.add("unknown", "u") // a tenant the guard has no tier for
	h.run(20, func(string, int) second { return second{accepted: 20, badCalls: 2} })

	if !slices.Equal(h.runtime.Killed, []string{"f"}) {
		t.Errorf("killed %v, want only the free tenant's sandbox", h.runtime.Killed)
	}
	h.expect("f", "4s bad-site kill")
	for _, id := range []string{"p", "u"} {
		// Egress is blocked, events still flow, and a human is asked.
		if want := (control.Policy{BlockEgress: true}); h.policy[id] != want {
			t.Errorf("%s: policy is %+v, want %+v", id, h.policy[id], want)
		}
		h.expect(id, "4s bad-site block_egress (approval_requested)")
	}
}

func TestBlastRadiusCapStopsKillingAndKeepsCheaperActions(t *testing.T) {
	h := newHarness(t, "max_kills_per_minute: 2\nrules:"+`
  - {name: slow-down, expr: "offered_rate > 100.0", for: 1s, action: throttle, rate: 10, mode: enforce, ttl: 30s}
  - {name: stop, expr: "offered_rate > 100.0", for: 3s, action: kill, mode: enforce, ttl: 30s}`,
		map[string]string{"t1": "free", "t2": "free", "t3": "free", "t4": "free"})
	h.add("t1", "s1")
	h.add("t2", "s2")
	h.add("t3", "s3")
	h.add("t4", "s4")
	h.run(10, steady(20))
	h.run(10, steady(500)) // a bad rule, or a real mass event: all four at once

	if !slices.Equal(h.runtime.Killed, []string{"s1", "s2"}) {
		t.Fatalf("killed %v, want two and then no more", h.runtime.Killed)
	}
	for _, id := range []string{"s3", "s4"} {
		if want := (control.Policy{Throttle: true, EventRate: 10}); h.policy[id] != want {
			t.Errorf("%s: policy is %+v, want the throttle to keep working", id, h.policy[id])
		}
		// Refused once and said once, not once per second.
		h.expect(id, "12s slow-down throttle", "14s stop  (blast_radius_cap)")
	}

	// A minute after the two kills the cap has room again, and the rule still fires.
	h.run(55, steady(500))
	if !slices.Equal(h.runtime.Killed, []string{"s1", "s2", "s3", "s4"}) {
		t.Errorf("killed %v, want the other two once the minute had passed", h.runtime.Killed)
	}
}

func TestDenyListIsTheKilledAndTheCutOff(t *testing.T) {
	h := newHarness(t, header+`
  - {name: slow, expr: "offered_rate > 100.0 && offered_rate < 200.0", action: throttle, rate: 10, mode: enforce, ttl: 1m}
  - {name: cut, expr: "offered_rate > 200.0 && offered_rate < 400.0", action: throttle, rate: 0, mode: enforce, ttl: 1m}
  - {name: kill, expr: "offered_rate > 400.0", action: kill, mode: enforce, ttl: 1m}
  - {name: block, expr: "watched_bytes_rate > 1000.0", action: block_egress, mode: enforce, ttl: 1m}`,
		map[string]string{"t": "free"})
	h.add("t", "quiet", "slowed", "cut", "killed", "blocked")
	traffic := func(id string, _ int) second {
		return map[string]second{
			"quiet": {accepted: 20}, "slowed": {accepted: 150}, "cut": {accepted: 300},
			"killed": {accepted: 500}, "blocked": {accepted: 20, watched: 5000},
		}[id]
	}
	h.run(5, traffic)
	// Throttled above zero and egress-blocked sandboxes keep their events:
	// blocking egress must not throw a tenant's data away.
	if got := h.g.Denied(); !slices.Equal(got, []string{"cut", "killed"}) {
		t.Errorf("deny list is %v, want [cut killed]", got)
	}
	if h.policy["blocked"] != (control.Policy{BlockEgress: true}) {
		t.Errorf("block_egress must not touch the event rate: %+v", h.policy["blocked"])
	}
}

func TestGuardThatRestartedKeepsWhatTheProxyEnforces(t *testing.T) {
	h := newHarness(t, header, nil)
	applied := control.Policy{Throttle: true, EventRate: 0, BlockEgress: true}
	// The first heartbeat this guard ever sees, from a proxy that was cut off before.
	policy, _ := h.g.Heartbeat(control.Heartbeat{TenantID: "t1", SandboxID: "s1", Applied: applied})
	if policy != applied {
		t.Fatalf("the restart loosened the policy: %+v", policy)
	}
	if got := h.g.Denied(); !slices.Equal(got, []string{"s1"}) {
		t.Errorf("deny list is %v, want [s1]", got)
	}
	// With no rule to renew it, it is lifted after a while.
	h.clk.Advance(adoptFor)
	h.g.Sweep()
	if policy, _ := h.g.Heartbeat(control.Heartbeat{TenantID: "t1", SandboxID: "s1", Applied: applied}); policy != (control.Policy{}) {
		t.Errorf("the adopted policy never expired: %+v", policy)
	}
	h.expect("s1", "0s adopted  (adopted)", "30s adopted  (lifted)")
}

type brokenRuntime struct{}

func (brokenRuntime) Kill(context.Context, string) error { return errors.New("docker is down") }

func TestKillThatFailsLeavesTheSandboxCutOff(t *testing.T) {
	h := newHarness(t, header+`
  - {name: stop, expr: "offered_rate > 100.0", action: kill, mode: enforce, ttl: 10s}
  - {name: later, expr: "offered_rate > 100.0", for: 20s, action: throttle, rate: 5, mode: enforce, ttl: 10s}`, map[string]string{"t1": "free"})
	h.g.cfg.Runtime = brokenRuntime{}
	h.add("t1", "s1")
	h.run(60, steady(500)) // far longer than the rule's ttl
	// Killed is final: the rules have nothing more to say about it.
	h.expect("s1", "3s stop kill")
	if h.policy["s1"] != control.CutOff {
		t.Errorf("policy is %+v, want everything cut off", h.policy["s1"])
	}
	if got := h.g.Denied(); !slices.Equal(got, []string{"s1"}) {
		t.Errorf("deny list is %v, want [s1]", got)
	}
}

func TestSlowIngestDoesNotLookLikeAnAttack(t *testing.T) {
	h := newHarness(t, header+`
  - {name: pushy, expr: "reject_ratio > 0.5 || offered_rate > 100.0", action: throttle, rate: 0, mode: enforce, ttl: 10s}`, nil)
	h.add("t1", "s1")
	// Ingest is slow: almost everything is refused with buffer_full.
	h.run(30, func(string, int) second { return second{accepted: 2, bufferFull: 400} })
	h.expect("s1")
}

func TestGuardRefusesMoreSandboxesThanItCanTrack(t *testing.T) {
	h := newHarness(t, header, nil)
	for i := range maxSandboxes {
		if _, err := h.g.Heartbeat(control.Heartbeat{TenantID: "t", SandboxID: fmt.Sprint("s", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.g.Heartbeat(control.Heartbeat{TenantID: "t", SandboxID: "one-more"}); !errors.Is(err, ErrFull) {
		t.Errorf("got %v, want ErrFull", err)
	}
	// The ones it already tracks are still served.
	if _, err := h.g.Heartbeat(control.Heartbeat{TenantID: "t", SandboxID: "s0"}); err != nil {
		t.Errorf("a tracked sandbox was refused: %v", err)
	}
}

func readRules(t *testing.T) string {
	t.Helper()
	text, err := os.ReadFile("../../rules/a.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}

// The shipped rule set against the shipped scenario, with made-up traffic of
// the same shape. Open loop: the traffic does not react to the guard.
func TestRuleSetAContainsEveryBadSandboxAndTouchesNoGoodOne(t *testing.T) {
	sc, err := scenario.Load("../../scenarios/default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, readRules(t), sc.Tiers())
	behaviour := map[string]scenario.Sandbox{}
	for _, tenant := range sc.Tenants {
		for _, s := range tenant.Sandboxes {
			h.add(tenant.ID, s.ID)
			behaviour[s.ID] = s
		}
	}
	h.run(75, func(id string, sec int) second {
		s := behaviour[id]
		if s.Behaviour == "bursty" && sec%20 < 2 {
			return second{accepted: 160} // inside the static limit, eight times its normal rate
		}
		if !s.Bad() || sec <= int(s.Start.Seconds()) {
			return second{accepted: 20}
		}
		switch {
		case s.Behaviour == "flood" && s.Rate > 0:
			return second{accepted: s.Rate}
		case s.Behaviour == "flood":
			return second{accepted: 100, rejected: 1900}
		case s.Behaviour == "exfil":
			return second{accepted: 20, watched: 1 << 20}
		}
		return second{accepted: 20, badCalls: 2} // baddest
	})

	for id := range behaviour {
		if n := len(h.decisions(id)); !behaviour[id].Bad() && n > 0 {
			t.Errorf("%s is well behaved and got %d decisions: %q", id, n, h.decisions(id))
		}
	}
	// The single flood starts at 10 s and climbs the whole ladder.
	h.expect("s11", "12s flood-seen observe", "13s flood throttle", "16s ignores-backpressure  (shadow)",
		"19s flood-goes-on throttle", "26s flood-never-stops kill")
	h.expect("s12", "23s exfil block_egress")
	h.expect("s13", "33s bad-site kill")
	// The paid tenant ends with egress blocked and an approval request, not a kill.
	h.expect("s14", "38s bad-site block_egress (approval_requested)")
	if h.policy["s14"] != (control.Policy{BlockEgress: true}) {
		t.Errorf("s14: policy is %+v", h.policy["s14"])
	}
	for _, id := range []string{"s15", "s16", "s17"} {
		if want := (control.Policy{Throttle: true, EventRate: 20}); h.policy[id] != want {
			t.Errorf("%s: policy is %+v, want %+v", id, h.policy[id], want)
		}
	}
	if !slices.Equal(h.runtime.Killed, []string{"s11", "s13"}) {
		t.Errorf("killed %v, want [s11 s13]", h.runtime.Killed)
	}
}
