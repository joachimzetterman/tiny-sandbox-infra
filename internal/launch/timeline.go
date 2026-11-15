package launch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// subject is one sandbox as the timeline sees it.
type subject struct {
	tenant    scenario.Tenant
	sandbox   scenario.Sandbox
	onset     time.Time     // when its behaviour starts
	announced bool          // "begins" was printed
	contained time.Duration // onset to the first enforced action, negative if none
}

// Watch prints the guard's decisions as they happen for the given time,
// then says how each misbehaving sandbox ended and whether the well-behaved
// ones were left alone. Launch knows when every behaviour starts, so it can
// measure how long containment took.
func (l *Launcher) Watch(ctx context.Context, sc *scenario.Scenario, length time.Duration, out io.Writer) error {
	var subjects []*subject
	byID := map[string]*subject{}
	for _, t := range sc.Tenants {
		for _, s := range t.Sandboxes {
			sub := &subject{tenant: t, sandbox: s, onset: l.started[s.ID].Add(s.Start), contained: -1}
			subjects = append(subjects, sub)
			byID[s.ID] = sub
		}
	}
	line := func(at time.Time, sub *subject, text string) {
		fmt.Fprintf(out, "%6.1fs  %-4s %-4s %-5s %s\n", at.Sub(l.began).Seconds(), sub.sandbox.ID, sub.tenant.ID, sub.tenant.Tier, text)
	}
	fmt.Fprintf(out, "\n%d sandboxes started. Watching the guard for %s.\n\n  time   sbx  tnt  tier  what happened\n", len(subjects), length)

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var seq uint64
	for end := l.Clock.Now().Add(length); l.Clock.Now().Before(end); {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		for _, sub := range subjects {
			if sub.sandbox.Bad() && !sub.announced && !l.Clock.Now().Before(sub.onset) {
				sub.announced = true
				line(sub.onset, sub, sub.sandbox.Behaviour+" begins")
			}
		}
		decisions, err := l.decisions(ctx, seq)
		if err != nil {
			fmt.Fprintf(out, "         guard unreachable: %v\n", err)
			continue
		}
		for _, d := range decisions {
			seq = d.Seq
			sub := byID[d.SandboxID]
			if sub == nil {
				continue // not from this scenario
			}
			text := describe(d) + "  (rule " + d.Rule + ")"
			enforced := d.Effect == guard.Throttle || d.Effect == guard.BlockEgress || d.Effect == guard.Kill
			if enforced && sub.contained < 0 {
				sub.contained = d.At.Sub(sub.onset)
				if sub.sandbox.Bad() {
					text += fmt.Sprintf("  %.1f s after onset", sub.contained.Seconds())
				}
			}
			line(d.At, sub, text)
		}
	}

	fmt.Fprintln(out, "\nSummary")
	good, touched := 0, 0
	for _, sub := range subjects {
		switch {
		case sub.sandbox.Bad() && sub.contained >= 0:
			fmt.Fprintf(out, "  %-4s %-8s contained %.1f s after onset\n", sub.sandbox.ID, sub.sandbox.Behaviour, sub.contained.Seconds())
		case sub.sandbox.Bad():
			fmt.Fprintf(out, "  %-4s %-8s NOT contained\n", sub.sandbox.ID, sub.sandbox.Behaviour)
		case sub.contained >= 0:
			good, touched = good+1, touched+1
			fmt.Fprintf(out, "  %-4s %-8s FALSE POSITIVE: a well-behaved sandbox was acted on\n", sub.sandbox.ID, sub.sandbox.Behaviour)
		default:
			good++
		}
	}
	fmt.Fprintf(out, "  neighbours: %d well-behaved sandboxes, %d throttled, blocked or killed\n\n", good, touched)
	return nil
}

// describe turns a decision into the words of the timeline.
func describe(d guard.Decision) string {
	switch {
	case d.Note == guard.NoteLifted:
		return "action lifted"
	case d.Note == guard.NoteAdopted:
		return "policy taken over from the proxy"
	case d.Note == guard.NoteShadow:
		return "would " + d.Action + " (shadow, nothing changed)"
	case d.Note == guard.NoteBlastRadius:
		return "kill refused: blast radius cap, a human is needed"
	case d.Note == guard.NoteApproval:
		return "egress blocked, kill left to a human (paid tenant)"
	case d.Effect == guard.Observe:
		return "detected"
	case d.Effect == guard.Throttle && d.Rate == 0:
		return "cut off: no events accepted"
	case d.Effect == guard.Throttle:
		return fmt.Sprintf("throttled to %g events/s", d.Rate)
	case d.Effect == guard.BlockEgress:
		return "egress blocked"
	}
	return "killed"
}

// decisions asks the guard for what it decided after number seq.
func (l *Launcher) decisions(ctx context.Context, seq uint64) ([]guard.Decision, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/decisions?after=%d", l.GuardURL, seq), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var decisions []guard.Decision
	return decisions, json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&decisions)
}
