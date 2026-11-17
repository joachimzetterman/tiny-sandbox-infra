// Package replay runs a recording through a rule set and reports what that
// rule set would have done. It drives the real guard, with the clock set to
// each recorded heartbeat and a runtime that kills nothing. replay.go does
// the run and report.go the Markdown.
//
// Replay is open loop. The traffic in a recording does not react to what
// the replayed guard decides: a sandbox it throttles or kills keeps sending
// as it did when it was recorded. What an action would have let through is
// therefore worked out here, from the policy in force over each second.
package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

type Recording struct {
	Header  guard.Header
	Records []guard.Record // in order of arrival
}

// maxSpan bounds the recorded time a replay steps through, one sweep per
// second.
const maxSpan = 24 * time.Hour

// Load reads a recording: a header line, then one record per line. A guard
// writes records in the order it judged them, which is also time order.
// Anything else did not come from a guard, and is refused.
func Load(r io.Reader) (*Recording, error) {
	lines := bufio.NewScanner(io.LimitReader(r, guard.MaxRecording+1<<20)) // the cap, and the line that crossed it
	lines.Buffer(nil, 1<<20)                                               // one line is one heartbeat, far smaller than this
	rec := &Recording{}
	if !lines.Scan() || json.Unmarshal(lines.Bytes(), &rec.Header) != nil || len(rec.Header.Tenants) == 0 {
		return nil, errors.New("a recording starts with a header line that lists the tenants")
	}
	for lines.Scan() {
		var record guard.Record
		n := len(rec.Records)
		if err := json.Unmarshal(lines.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("record %d: %w", n+1, err)
		}
		switch {
		case record.At.IsZero() || record.SandboxID == "":
			return nil, fmt.Errorf("record %d has no time or no sandbox", n+1)
		case n > 0 && record.At.Before(rec.Records[n-1].At):
			return nil, fmt.Errorf("record %d is earlier than the one before it", n+1)
		case n > 0 && record.At.Sub(rec.Records[0].At) > maxSpan:
			return nil, fmt.Errorf("record %d is more than %s after the first", n+1, maxSpan)
		}
		rec.Records = append(rec.Records, record)
	}
	if len(rec.Records) == 0 || lines.Err() != nil {
		return nil, fmt.Errorf("no records, or a line that is too long: %v", lines.Err())
	}
	return rec, nil
}

// Outcome is what one rule set did about one sandbox.
type Outcome struct {
	TenantID, Tier string
	Sandbox        scenario.Sandbox
	// For a misbehaving sandbox, counted from the moment its behaviour
	// began. A negative time means it never happened. Contained is the
	// first heartbeat after onset that was answered with a restriction,
	// whether that was decided then or held from before.
	First, Contained     time.Duration // to the first action, and to the first that restricts the sandbox
	FirstBy, ContainedBy string        // what was done and by which rule
	Passed               uint64        // events let through after onset
	Egress               uint64        // bytes sent after onset while egress was open
	// Reached is set when the recording goes on past the sandbox's onset.
	Reached bool
	// Wronged is set when the sandbox was restricted while it was well
	// behaved: a false positive. It says by what.
	Wronged string
}

// Misbehaved says the sandbox is a bad one and the recording shows it. A
// bad sandbox whose behaviour begins after the recording ends is not scored.
func (o *Outcome) Misbehaved() bool { return o.Sandbox.Bad() && o.Reached }

// Result is one rule set against one recording.
type Result struct {
	Outcomes  []*Outcome // one per sandbox, in the order of the scenario
	Decisions []guard.Decision
	CapHits   int                      // kills the blast radius cap refused
	Failures  map[string]guard.Failure // rules that could not be evaluated
}

// following is what the replay remembers about one sandbox between two of
// its heartbeats.
type following struct {
	out              *Outcome
	onset            time.Time
	at               time.Time        // the last heartbeat, or before the first, when the proxy started
	c                control.Counters // the counters then
	policy           control.Policy   // and what the guard answered
	passed, egress   float64
	contained, since time.Time // when it was contained, and when the action that did it was taken
}

// collector is the guard's store during a replay: it keeps the decisions
// and throws the usage rows away.
type collector struct{ decisions []guard.Decision }

func (c *collector) InsertActions(_ context.Context, rows []guard.Decision) error {
	c.decisions = append(c.decisions, rows...)
	return nil
}
func (*collector) InsertUsage(context.Context, []guard.Usage) error { return nil }

// Run replays a recording through one rule set.
func Run(rec *Recording, rules *guard.RuleSet) *Result {
	ctx, begin := context.Background(), rec.Records[0].At
	clk, sink, res := clock.NewFake(begin), &collector{}, &Result{}
	tiers, sandboxes := map[string]string{}, map[string]*following{}
	for _, t := range rec.Header.Tenants {
		tiers[t.ID] = t.Tier
		for _, s := range t.Sandboxes {
			out := &Outcome{TenantID: t.ID, Tier: t.Tier, Sandbox: s, First: -1, Contained: -1}
			res.Outcomes = append(res.Outcomes, out)
			sandboxes[s.ID] = &following{out: out}
		}
	}
	g := guard.New(guard.Config{Rules: rules, Tiers: tiers, Runtime: &guard.Nothing{}, Sink: sink, Clock: clk})

	tick := begin.Add(time.Second)
	for _, r := range rec.Records {
		// A live guard lifts expired actions and writes its rows on timers.
		// Here that happens at every whole second of recorded time, so the
		// same recording always gives the same result.
		for ; !tick.After(r.At); tick = tick.Add(time.Second) {
			clk.Advance(tick.Sub(clk.Now()))
			g.Sweep()
			g.Flush(ctx)
		}
		clk.Advance(r.At.Sub(clk.Now()))
		hb := r.Heartbeat
		hb.Applied = control.Policy{} // nobody enforced what this guard decided
		policy, err := g.Heartbeat(hb)
		f := sandboxes[hb.SandboxID]
		if err != nil || f == nil {
			continue // not in the scenario: judged, but not reported on
		}
		if f.onset.IsZero() {
			// The behaviour starts at an offset from the sandbox's start,
			// and the nearest thing on record is when its proxy started.
			// Its counters started from zero then, with nothing held.
			f.at = hb.Started
			if f.at.IsZero() || f.at.After(r.At) {
				f.at = r.At // not known: the first heartbeat stands in, and what it counted is lost
			}
			f.onset = f.at.Add(f.out.Sandbox.Start)
		}
		f.count(r.At, hb.Counters, policy)
	}
	g.Flush(ctx)
	res.Decisions, res.Failures = sink.decisions, g.Failures()

	for _, d := range res.Decisions {
		if d.Note == guard.NoteBlastRadius {
			res.CapHits++
		}
		f := sandboxes[d.SandboxID]
		if f == nil || d.Effect == "" {
			continue // shadow, lifted and adopted change nothing
		}
		out, since, restricts := f.out, d.At.Sub(f.onset), d.Effect != guard.Observe
		by := d.Effect + " by `" + d.Rule + "`"
		switch {
		case out.Sandbox.Bad() && since >= 0:
			if out.First < 0 {
				out.First, out.FirstBy = since, by
			}
		case restricts && out.Wronged == "":
			out.Wronged = fmt.Sprintf("%s at %.1f s", by, d.At.Sub(begin).Seconds())
		}
		// What contained it: the latest restriction taken by then.
		if restricts && out.Contained >= 0 && !d.At.After(f.contained) && (out.ContainedBy == "" || d.At.After(f.since)) {
			out.ContainedBy, f.since = by, d.At
		}
	}
	for _, f := range sandboxes {
		out := f.out
		out.Passed, out.Egress = uint64(math.Round(f.passed)), uint64(math.Round(f.egress))
		if out.Contained >= 0 && f.since.Before(f.onset) {
			out.ContainedBy += ", held from before onset"
		}
		// A restriction already in force at onset is also the first thing
		// done about the behaviour.
		if out.Contained >= 0 && (out.First < 0 || out.Contained < out.First) {
			out.First, out.FirstBy = out.Contained, out.ContainedBy
		}
	}
	return res
}

// count takes one heartbeat. The traffic since the last one is taken as
// spread evenly over that interval, and only the part after onset counts:
// what the policy in force then would have let through.
func (f *following) count(at time.Time, c control.Counters, policy control.Policy) {
	from := f.at
	if from.Before(f.onset) {
		from = f.onset
	}
	if span, after := at.Sub(f.at), at.Sub(from); span > 0 && after > 0 {
		share := after.Seconds() / span.Seconds()
		passed := float64(grown(c.EventsAccepted, f.c.EventsAccepted)) * share
		if f.policy.Throttle {
			passed = min(passed, f.policy.EventRate*after.Seconds())
		}
		f.passed += passed
		if !f.policy.BlockEgress {
			f.egress += float64(grown(control.Total(c.EgressByClass), control.Total(f.c.EgressByClass))) * share
		}
		f.out.Reached = true
	}
	if f.out.Sandbox.Bad() && !at.Before(f.onset) && policy != (control.Policy{}) && f.out.Contained < 0 {
		f.out.Contained, f.contained = at.Sub(f.onset), at
	}
	f.at, f.c, f.policy = at, c, policy
}

// grown is how much a counter grew. One that went down belongs to a proxy
// that restarted and counted from zero again.
func grown(cur, old uint64) uint64 {
	if cur < old {
		return cur
	}
	return cur - old
}
