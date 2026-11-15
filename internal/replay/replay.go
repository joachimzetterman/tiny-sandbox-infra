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
	"slices"
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

// Load reads a recording: a header line, then one record per line.
func Load(r io.Reader) (*Recording, error) {
	lines := bufio.NewScanner(io.LimitReader(r, guard.MaxRecording+1<<20)) // the cap, and the line that crossed it
	lines.Buffer(nil, 1<<20)                                               // one line is one heartbeat, far smaller than this
	rec := &Recording{}
	if !lines.Scan() || json.Unmarshal(lines.Bytes(), &rec.Header) != nil || len(rec.Header.Tenants) == 0 {
		return nil, errors.New("a recording starts with a header line that lists the tenants")
	}
	for lines.Scan() {
		var record guard.Record
		if err := json.Unmarshal(lines.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("record %d: %w", len(rec.Records)+1, err)
		}
		rec.Records = append(rec.Records, record)
	}
	if len(rec.Records) == 0 || lines.Err() != nil {
		return nil, fmt.Errorf("no records, or a line that is too long: %v", lines.Err())
	}
	// Already in order when a guard wrote it. Sorting makes that a fact.
	slices.SortStableFunc(rec.Records, func(a, b guard.Record) int { return a.At.Compare(b.At) })
	return rec, nil
}

// Outcome is what one rule set did about one sandbox.
type Outcome struct {
	TenantID, Tier string
	Sandbox        scenario.Sandbox
	// For a misbehaving sandbox, counted from the moment its behaviour
	// began. A negative time means it never happened.
	First, Contained     time.Duration // to the first decision, and to the first that restricts the sandbox
	FirstBy, ContainedBy string        // what was done and by which rule
	Passed               uint64        // events let through after onset
	Egress               uint64        // bytes sent after onset while egress was open
	// Wronged is set when the sandbox was restricted while it was well
	// behaved: a false positive. It says by what.
	Wronged string
}

// Result is one rule set against one recording.
type Result struct {
	Outcomes  []*Outcome // one per sandbox, in the order of the scenario
	Decisions []guard.Decision
	CapHits   int // kills the blast radius cap refused
}

// following is what the replay remembers about one sandbox between two of
// its heartbeats.
type following struct {
	out    *Outcome
	onset  time.Time
	at     time.Time        // the last heartbeat
	c      control.Counters // and its counters
	policy control.Policy   // what the guard answered to it
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
		switch {
		case f.at.IsZero():
			// The behaviour starts at an offset from the sandbox's start,
			// and the nearest thing on record is when its proxy started.
			started := hb.Started
			if started.IsZero() {
				started = r.At
			}
			f.onset = started.Add(f.out.Sandbox.Start)
		case !r.At.Before(f.onset):
			// What the policy of the last second would have let through.
			passed := float64(grown(hb.Counters.EventsAccepted, f.c.EventsAccepted))
			if f.policy.Throttle {
				passed = min(passed, f.policy.EventRate*r.At.Sub(f.at).Seconds())
			}
			f.out.Passed += uint64(passed)
			if !f.policy.BlockEgress {
				f.out.Egress += grown(control.Total(hb.Counters.EgressByClass), control.Total(f.c.EgressByClass))
			}
		}
		f.at, f.c, f.policy = r.At, hb.Counters, policy
	}
	g.Flush(ctx)

	res.Decisions = sink.decisions
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
			if restricts && out.Contained < 0 {
				out.Contained, out.ContainedBy = since, by
			}
		case restricts && out.Wronged == "":
			out.Wronged = fmt.Sprintf("%s at %.1f s", by, d.At.Sub(begin).Seconds())
		}
	}
	return res
}

// grown is how much a counter grew. One that went down belongs to a proxy
// that restarted and counted from zero again.
func grown(cur, old uint64) uint64 {
	if cur < old {
		return cur
	}
	return cur - old
}
