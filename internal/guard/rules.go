package guard

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/cel-go/cel"
	"go.yaml.in/yaml/v3"
)

// The ladder, cheapest first.
const (
	Observe     = "observe"      // record, change nothing
	Throttle    = "throttle"     // shrink the event rate. Rate zero is the cut-off.
	BlockEgress = "block_egress" // refuse outbound calls. Events are untouched.
	Kill        = "kill"         // stop the sandbox and its proxy
)

const (
	Shadow  = "shadow"  // record what the rule would have done
	Enforce = "enforce" // do it
)

// Variables is the fixed set of names a rule expression can use. All are
// numbers. One more, tier, is the string "free" or "paid".
var Variables = []string{
	"offered_rate",         // events per second this sandbox tried to send
	"accepted_rate",        // events per second that got through
	"reject_ratio",         // share of offered events refused, 0 to 1
	"egress_bytes_rate",    // bytes per second to allowed sites
	"watched_bytes_rate",   // bytes per second to watched sites
	"bad_site_attempts",    // calls to bad sites in the window
	"baseline_rate",        // this sandbox's own normal offered_rate
	"tenant_offered_rate",  // offered_rate summed over the tenant's sandboxes
	"tenant_baseline_rate", // baseline_rate summed over the tenant's sandboxes
	"tenant_sandboxes",     // how many of the tenant's sandboxes are reporting
}

// Rule is one line of policy: when Expr has been true for For, take Action
// and keep it for TTL after the last time the rule fired.
type Rule struct {
	Name   string        `yaml:"name"`
	Expr   string        `yaml:"expr"`   // CEL, over Variables
	For    time.Duration `yaml:"for"`    // how long Expr must hold
	Action string        `yaml:"action"` // a step of the ladder
	Rate   *float64      `yaml:"rate"`   // throttle only, and required there: the new events per second
	Mode   string        `yaml:"mode"`   // shadow or enforce
	TTL    time.Duration `yaml:"ttl"`    // the action expires unless the rule still fires

	program cel.Program
}

type RuleSet struct {
	// MaxKillsPerMinute is the blast radius cap: above it automation stops
	// killing and asks for a human.
	MaxKillsPerMinute int     `yaml:"max_kills_per_minute"`
	Rules             []*Rule `yaml:"rules"`
}

func LoadRules(path string) (*RuleSet, error) {
	text, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseRules(text)
}

// ParseRules reads and checks a rule set. Any mistake is an error: a guard
// that starts with half its rules is worse than one that does not start.
func ParseRules(text []byte) (*RuleSet, error) {
	dec := yaml.NewDecoder(bytes.NewReader(text))
	dec.KnownFields(true)
	var rs RuleSet
	if err := dec.Decode(&rs); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}
	if rs.MaxKillsPerMinute <= 0 {
		return nil, fmt.Errorf("max_kills_per_minute must be positive")
	}
	// Declaring every variable with its type means a typo in a rule is an
	// error here, at load time, and not a rule that silently never fires.
	opts := []cel.EnvOption{cel.Variable("tier", cel.StringType), cel.CrossTypeNumericComparisons(true)}
	for _, name := range Variables {
		opts = append(opts, cel.Variable(name, cel.DoubleType))
	}
	env, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, r := range rs.Rules {
		if r.Mode == "" {
			r.Mode = Shadow // a rule nobody marked as enforcing changes nothing
		}
		switch {
		case r.Name == "" || seen[r.Name]:
			return nil, fmt.Errorf("rule name %q is empty or used twice", r.Name)
		case r.Action != Observe && r.Action != Throttle && r.Action != BlockEgress && r.Action != Kill:
			return nil, fmt.Errorf("rule %s: unknown action %q", r.Name, r.Action)
		case r.Mode != Shadow && r.Mode != Enforce:
			return nil, fmt.Errorf("rule %s: mode must be shadow or enforce, got %q", r.Name, r.Mode)
		case r.For < 0 || r.TTL <= 0:
			return nil, fmt.Errorf("rule %s: for must not be negative and ttl must be positive", r.Name)
		// A pointer, so a rate left out is not read as rate 0, the cut-off.
		case (r.Rate != nil) != (r.Action == Throttle) || (r.Rate != nil && *r.Rate < 0):
			return nil, fmt.Errorf("rule %s: throttle needs a rate, nothing else takes one, and it must not be negative", r.Name)
		}
		seen[r.Name] = true
		ast, issues := env.Compile(r.Expr)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("rule %s: %w", r.Name, issues.Err())
		}
		if !ast.OutputType().IsExactType(cel.BoolType) {
			return nil, fmt.Errorf("rule %s: expr must be true or false", r.Name)
		}
		if r.program, err = env.Program(ast); err != nil {
			return nil, fmt.Errorf("rule %s: %w", r.Name, err)
		}
	}
	return &rs, nil
}

// holds reports whether the rule's expression is true for these inputs.
func (r *Rule) holds(inputs map[string]any) bool {
	out, _, err := r.program.Eval(inputs)
	if err != nil {
		slog.Warn("rule could not be evaluated, treating it as false", "rule", r.Name, "err", err)
		return false
	}
	return out.Value() == true
}
