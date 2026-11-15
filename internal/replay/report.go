package replay

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
)

// AB replays one recording through two rule sets and writes the report.
func AB(w io.Writer, recording, a, b string) error {
	f, err := os.Open(recording)
	if err != nil {
		return err
	}
	defer f.Close()
	rec, err := Load(f)
	if err != nil {
		return fmt.Errorf("%s: %w", recording, err)
	}
	var results [2]*Result
	for i, path := range []string{a, b} {
		rules, err := guard.LoadRules(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		results[i] = Run(rec, rules)
	}
	Report(w, recording, rec, [2]string{a, b}, results)
	return nil
}

// tally is one rule set's result in a few numbers and lists.
type tally struct {
	bad, contained int
	misses, wrongs []string
	passed, egress uint64
}

func count(res *Result) (t tally) {
	for _, o := range res.Outcomes {
		if o.Wronged != "" {
			t.wrongs = append(t.wrongs, fmt.Sprintf("%s (%s, %s) %s", o.Sandbox.ID, o.Sandbox.Behaviour, o.Tier, o.Wronged))
		}
		if !o.Sandbox.Bad() {
			continue
		}
		t.bad, t.passed, t.egress = t.bad+1, t.passed+o.Passed, t.egress+o.Egress
		if o.Contained >= 0 {
			t.contained++
		} else {
			t.misses = append(t.misses, o.Sandbox.ID+" ("+o.Sandbox.Behaviour+")")
		}
	}
	return t
}

// Report writes the Markdown: one section per rule set, then the two side
// by side.
func Report(w io.Writer, name string, rec *Recording, paths [2]string, results [2]*Result) {
	tallies := [2]tally{count(results[0]), count(results[1])}
	length := rec.Records[len(rec.Records)-1].At.Sub(rec.Records[0].At)
	fmt.Fprintf(w, "# A/B replay of %s\n\n%d sandboxes, %d of them misbehaving, %.0f s of heartbeats.\n",
		name, len(results[0].Outcomes), tallies[0].bad, length.Seconds())
	if rec.Header.Enforced {
		fmt.Fprintln(w, "\n**Rules were enforcing while this was recorded, so the traffic is already shaped by one rule set. Record with `make record` for a fair comparison.**")
	}
	for i, res := range results {
		fmt.Fprintf(w, "\n## Rule set %c: %s\n\n", "AB"[i], paths[i])
		fmt.Fprintln(w, "| Sandbox | Tenant | Tier | Behaviour | First action | Contained | Events passed after onset | Egress before the block |\n|---|---|---|---|---|---|---|---|")
		for _, o := range res.Outcomes {
			if o.Sandbox.Bad() {
				fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %d | %s |\n", o.Sandbox.ID, o.TenantID, o.Tier, o.Sandbox.Behaviour,
					after(o.First, o.FirstBy), after(o.Contained, o.ContainedBy), o.Passed, kib(o.Egress))
			}
		}
		fmt.Fprintf(w, "\n- Misses: %s\n- False positives: %s\n- Blast radius cap: %s\n", list(tallies[i].misses), list(tallies[i].wrongs), capped(res))
	}

	fmt.Fprintln(w, "\n## A against B\n\n| | A | B |\n|---|---|---|")
	for i, o := range results[0].Outcomes {
		if o.Sandbox.Bad() {
			fmt.Fprintf(w, "| %s %s contained after | %s | %s |\n", o.Sandbox.ID, o.Sandbox.Behaviour, after(o.Contained, ""), after(results[1].Outcomes[i].Contained, ""))
		}
	}
	a, b := tallies[0], tallies[1]
	fmt.Fprintf(w, "| Bad sandboxes contained | %d of %d | %d of %d |\n", a.contained, a.bad, b.contained, b.bad)
	fmt.Fprintf(w, "| Misses | %d | %d |\n| False positives | %d | %d |\n", len(a.misses), len(b.misses), len(a.wrongs), len(b.wrongs))
	fmt.Fprintf(w, "| Events passed after onset | %d | %d |\n| Egress before the block | %s | %s |\n", a.passed, b.passed, kib(a.egress), kib(b.egress))
	fmt.Fprintf(w, "| Blast radius cap | %s | %s |\n", capped(results[0]), capped(results[1]))
	fmt.Fprintln(w, `
Times count from the moment a behaviour began. "First action" is the first decision of any kind, "contained" the first that throttles, blocks egress or kills.

**Replay is open loop.** The recorded traffic does not react to what a rule set decides: a sandbox that is throttled or killed here keeps sending as it did when it was recorded. "Events passed" and "egress before the block" are therefore estimates: the recorded traffic, cut down to what the policy in force over each second would have let through.`)
}

// after writes a time since onset, and what was done if that is given.
func after(d time.Duration, by string) string {
	switch {
	case d < 0:
		return "**never**"
	case by == "":
		return fmt.Sprintf("%.1f s", d.Seconds())
	}
	return fmt.Sprintf("%.1f s: %s", d.Seconds(), by)
}

func kib(n uint64) string { return fmt.Sprintf("%d KiB", n>>10) }

func list(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return fmt.Sprintf("**%d**: %s", len(items), strings.Join(items, ", "))
}

func capped(res *Result) string {
	if res.CapHits == 0 {
		return "not reached"
	}
	return fmt.Sprintf("**reached**, %d kills refused", res.CapHits)
}
