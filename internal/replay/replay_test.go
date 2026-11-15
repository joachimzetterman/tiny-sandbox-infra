package replay

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

const (
	recordings = "../../recordings/"
	rulesA     = "../../rules/a.yaml"
	rulesB     = "../../rules/b.yaml"
)

var start = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler)) // the guard logs every decision
	os.Exit(m.Run())
}

func load(t *testing.T, name string) *Recording {
	t.Helper()
	f, err := os.Open(recordings + name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rec, err := Load(f)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func rules(t *testing.T, path string) *guard.RuleSet {
	t.Helper()
	rs, err := guard.LoadRules(path)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

func parse(t *testing.T, text string) *guard.RuleSet {
	t.Helper()
	rs, err := guard.ParseRules([]byte("max_kills_per_minute: 3\nrules:\n" + text))
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

// outcomes runs one rule set and returns the outcome of each sandbox by ID.
func outcomes(rec *Recording, rs *guard.RuleSet) (map[string]*Outcome, *Result) {
	res, byID := Run(rec, rs), map[string]*Outcome{}
	for _, o := range res.Outcomes {
		byID[o.Sandbox.ID] = o
	}
	return byID, res
}

func TestSameRecordingGivesTheSameReportEveryTime(t *testing.T) {
	for name, lines := range map[string][]string{
		"default.ndjson": {
			"| Bad sandboxes contained | 7 of 7 | 4 of 7 |",
			"- Misses: **3**: s15 (flood), s16 (flood), s17 (flood)",
			"- False positives: **1**: s10 (bursty, paid) throttle by `flood`",
			"| False positives | 0 | 1 |",
			"Replay is open loop",
		},
		"mass.ndjson": {"- Blast radius cap: **reached**, 2 kills refused", "| Misses | 0 | 0 |"},
	} {
		var first string
		for range 3 {
			var out bytes.Buffer
			if err := AB(&out, recordings+name, rulesA, rulesB); err != nil {
				t.Fatal(err)
			}
			if first == "" {
				first = out.String()
			}
			if out.String() != first {
				t.Fatalf("%s: two replays of the same recording gave different reports", name)
			}
		}
		for _, line := range lines {
			if !strings.Contains(first, line) {
				t.Errorf("%s: the report lacks %q", name, line)
			}
		}
	}
}

// A live guard that enforces and records, then the recording replayed
// through the same rules: the replay must reach the very same decisions, at
// the same times and on the same numbers.
func TestReplayDecidesWhatTheLiveGuardDecided(t *testing.T) {
	sc, err := scenario.Load("../../scenarios/default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var tape bytes.Buffer
	if err := json.NewEncoder(&tape).Encode(guard.Header{Enforced: true, Tenants: sc.Tenants}); err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(start)
	live := guard.New(guard.Config{Rules: rules(t, rulesA), Tiers: sc.Tiers(), Runtime: &guard.Nothing{}, Record: &tape, Clock: clk})
	totals := map[string]*control.Counters{}
	const seconds = 100
	for sec := 1; sec <= seconds; sec++ {
		clk.Advance(time.Second)
		live.Sweep()
		for _, tenant := range sc.Tenants {
			for _, s := range tenant.Sandboxes {
				c := totals[s.ID]
				if c == nil {
					c = &control.Counters{Rejects: map[event.Reason]uint64{}, EgressByClass: map[string]uint64{}, Blocked: map[event.Reason]uint64{}}
					totals[s.ID] = c
				}
				c.EventsAccepted += 20
				if s.Bad() && sec > int(s.Start.Seconds()) {
					switch {
					case s.Behaviour == "flood" && s.Rate > 0: // part of the split flood
						if sec <= 55 { // which ends, so its throttle is lifted before the test is over
							c.EventsAccepted += uint64(s.Rate) - 20
						}
					case s.Behaviour == "flood":
						c.EventsAccepted += 80
						c.Rejects[event.RateLimited] += 1900
					case s.Behaviour == "exfil":
						c.EgressByClass[scenario.ClassWatched] += 1 << 20
					default:
						c.Blocked[event.SiteBad] += 2
					}
				}
				sent := *c // a copy of the maps too, as if it had gone over the wire
				sent.Rejects = map[event.Reason]uint64{event.RateLimited: c.Rejects[event.RateLimited]}
				sent.EgressByClass = map[string]uint64{scenario.ClassWatched: c.EgressByClass[scenario.ClassWatched]}
				sent.Blocked = map[event.Reason]uint64{event.SiteBad: c.Blocked[event.SiteBad]}
				if _, err := live.Heartbeat(control.Heartbeat{TenantID: tenant.ID, SandboxID: s.ID, Started: start, Counters: sent}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	rec, err := Load(&tape)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Header.Enforced || len(rec.Records) != seconds*17 {
		t.Fatalf("the recording has enforced=%v and %d records, want true and %d", rec.Header.Enforced, len(rec.Records), seconds*17)
	}
	res := Run(rec, rules(t, rulesA))
	want, got := live.Since(0), res.Decisions
	if len(want) < 14 || want[len(want)-1].Note != guard.NoteLifted {
		t.Fatalf("the live guard made %d decisions and the last is %+v: the test should cover a lifted action", len(want), want[len(want)-1])
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("replay made %d decisions, the live guard %d, or they differ:\n got %+v\nwant %+v", len(got), len(want), got, want)
	}
	// This recording was made with the rules enforcing. The report must say so.
	var report bytes.Buffer
	Report(&report, "live", rec, [2]string{"a", "a"}, [2]*Result{res, res})
	if !strings.Contains(report.String(), "Rules were enforcing while this was recorded") {
		t.Error("the report does not warn that the recording was made with rules enforcing")
	}
}

func TestShippedRuleSetsShowTheTradeoff(t *testing.T) {
	rec := load(t, "default.ndjson")
	a, resA := outcomes(rec, rules(t, rulesA))
	b, resB := outcomes(rec, rules(t, rulesB))

	// A: every bad sandbox contained, nobody else touched.
	for id, o := range a {
		if o.Sandbox.Bad() && o.Contained < 0 {
			t.Errorf("A never contained %s (%s)", id, o.Sandbox.Behaviour)
		}
		if o.Wronged != "" {
			t.Errorf("A: false positive on %s: %s", id, o.Wronged)
		}
	}
	// A notices the flood before it acts on it: observing is not containing.
	if a["s11"].First >= a["s11"].Contained || a["s11"].FirstBy != "observe by `flood-seen`" {
		t.Errorf("A on s11: first action after %s (%s), contained after %s", a["s11"].First, a["s11"].FirstBy, a["s11"].Contained)
	}
	// B is faster on everything it can see, and lets less of the flood through.
	for _, id := range []string{"s11", "s12", "s13", "s14"} {
		if b[id].Contained < 0 || b[id].Contained >= a[id].Contained {
			t.Errorf("%s: B contained after %s, A after %s, want B sooner", id, b[id].Contained, a[id].Contained)
		}
	}
	if b["s11"].Passed >= a["s11"].Passed || b["s12"].Egress >= a["s12"].Egress {
		t.Errorf("B should let less through: events %d against %d, egress %d against %d",
			b["s11"].Passed, a["s11"].Passed, b["s12"].Egress, a["s12"].Egress)
	}
	// B pays for it: it throttles the paying tenant's legitimate burst, and
	// it cannot see the split flood.
	if !strings.Contains(b["s10"].Wronged, "throttle by `flood`") {
		t.Errorf("B: want a false positive on the bursty s10, got %q", b["s10"].Wronged)
	}
	for _, id := range []string{"s15", "s16", "s17"} {
		if b[id].Contained >= 0 {
			t.Errorf("B contained %s, part of the split flood, by %s", id, b[id].ContainedBy)
		}
	}
	for id, o := range b {
		if id != "s10" && o.Wronged != "" {
			t.Errorf("B: unexpected false positive on %s: %s", id, o.Wronged)
		}
	}
	if resA.CapHits+resB.CapHits != 0 {
		t.Errorf("the blast radius cap was reached %d and %d times, want never", resA.CapHits, resB.CapHits)
	}
}

func TestMassEventReachesTheBlastRadiusCap(t *testing.T) {
	byID, res := outcomes(load(t, "mass.ndjson"), rules(t, rulesA))
	kills := 0
	for _, d := range res.Decisions {
		if d.Effect == guard.Kill {
			kills++
		}
	}
	// Five sandboxes earn a kill at once and the cap allows three a minute.
	// The other two have their egress blocked, so all five are contained.
	if kills != 3 || res.CapHits != 2 {
		t.Errorf("%d kills and %d refused, want 3 and 2", kills, res.CapHits)
	}
	for id, o := range byID {
		if o.Sandbox.Bad() == (o.Contained < 0) || o.Wronged != "" {
			t.Errorf("%s (%s): contained after %s, false positive %q", id, o.Sandbox.Behaviour, o.Contained, o.Wronged)
		}
	}
}

// A hand-made recording with round numbers, to check the arithmetic of what
// a policy would have let through.
func TestEventsAndEgressAreCountedUnderThePolicyInForce(t *testing.T) {
	rec := &Recording{Header: guard.Header{Tenants: []scenario.Tenant{{ID: "t", Tier: scenario.TierFree,
		Sandboxes: []scenario.Sandbox{{ID: "x", Behaviour: "flood", Start: 10 * time.Second}}}}}}
	var c control.Counters
	for sec := 1; sec <= 30; sec++ {
		c.EventsAccepted += 20
		if sec > 10 { // the flood: 100 a second get through the static limit, 1,900 do not
			c.EventsAccepted += 80
		}
		rejected, egress := uint64(max(0, sec-10))*1900, uint64(sec)*1000
		rec.Records = append(rec.Records, guard.Record{At: start.Add(time.Duration(sec) * time.Second), Heartbeat: control.Heartbeat{
			TenantID: "t", SandboxID: "x", Started: start, Counters: control.Counters{EventsAccepted: c.EventsAccepted,
				Rejects: map[event.Reason]uint64{event.RateLimited: rejected}, EgressByClass: map[string]uint64{scenario.ClassTrusted: egress}},
			// As if recorded while some other rule set had cut it off. The
			// replay must not take that policy over.
			Applied: control.CutOff,
		}})
	}
	// The shadow rule would kill at once. It changes nothing, so it must
	// not count as an action either.
	res := Run(rec, parse(t, `
  - {name: candidate, expr: "offered_rate > 100.0", action: kill, mode: shadow, ttl: 1m}
  - {name: slow, expr: "offered_rate > 100.0", action: throttle, rate: 10, mode: enforce, ttl: 1m}
  - {name: block, expr: "offered_rate > 100.0", for: 5s, action: block_egress, mode: enforce, ttl: 1m}`))
	o := res.Outcomes[0]
	// Onset is at 10 s. The window shows the flood at 11 s and `slow` acts.
	if o.First != time.Second || o.Contained != time.Second || o.ContainedBy != "throttle by `slow`" {
		t.Errorf("first action after %s, contained after %s by %q, want 1s, 1s, throttle by `slow`", o.First, o.Contained, o.ContainedBy)
	}
	// The second that ends at onset passes 20, the first of the flood 100,
	// and each of the 19 after that the 10 the throttle allows.
	if o.Passed != 20+100+19*10 {
		t.Errorf("%d events passed after onset, want 310", o.Passed)
	}
	// Egress is 1,000 bytes a second and is blocked from 16 s on.
	if o.Egress != 7000 {
		t.Errorf("%d bytes of egress before the block, want 7000", o.Egress)
	}

	// A recording that does not say when the proxy started: the first
	// heartbeat, one second in, stands in for it, so onset moves to 11 s.
	for i := range rec.Records {
		rec.Records[i].Started = time.Time{}
	}
	if o := Run(rec, parse(t, `
  - {name: slow, expr: "offered_rate > 100.0", action: throttle, rate: 10, mode: enforce, ttl: 1m}`)).Outcomes[0]; o.Contained != 0 {
		t.Errorf("contained after %s, want 0s", o.Contained)
	}
	// A counter that went down is a proxy that began again from zero.
	if grown(40, 5100) != 40 || grown(50, 40) != 10 {
		t.Error("grown does not treat a counter that went down as a fresh start")
	}

	// A rule that acts at 3 s, long before the sandbox misbehaves, is a
	// false positive and not a fast containment.
	o = Run(rec, parse(t, `
  - {name: eager, expr: "offered_rate > 5.0", action: throttle, rate: 5, mode: enforce, ttl: 1m}`)).Outcomes[0]
	if o.Wronged != "throttle by `eager` at 2.0 s" || o.Contained >= 0 {
		t.Errorf("false positive %q, contained after %s, want the early throttle and never", o.Wronged, o.Contained)
	}
}

func TestBrokenRecordingIsRefused(t *testing.T) {
	header := `{"enforced":false,"tenants":[{"id":"t","tier":"free","sandboxes":[{"id":"x","behaviour":"normal"}]}]}` + "\n"
	record := `{"at":"2026-01-02T03:04:06Z","tenant_id":"t","sandbox_id":"x","counters":{"events_accepted":20}}` + "\n"
	if _, err := Load(strings.NewReader(header + record)); err != nil {
		t.Fatalf("a good recording was refused: %v", err)
	}
	for what, text := range map[string]string{
		"an empty file":        "",
		"a header and nothing": header,
		"no header":            record + record,
		"a line that is cut":   header + record + record[:40],
	} {
		if _, err := Load(strings.NewReader(text)); err == nil {
			t.Errorf("%s was accepted", what)
		}
	}
	// Records out of order are put in order, so time never runs backwards.
	late := strings.Replace(record, "03:04:06", "03:04:09", 1)
	rec, err := Load(strings.NewReader(header + late + record))
	if err != nil || !rec.Records[0].At.Before(rec.Records[1].At) {
		t.Errorf("records were not sorted by time: %v, %v", rec, err)
	}
}
