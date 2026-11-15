// Package guard watches every sandbox through its proxy's heartbeats and
// contains the ones that misbehave. window.go turns counters into rates,
// rules.go holds the rule language, this file decides, server.go is the
// HTTP and storage around it, and recorder.go keeps what it saw for replay.
package guard

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// Runtime is the one thing the guard can do outside its own memory: stop a
// sandbox and its proxy. Docker in the demo, Nothing in replay and tests.
type Runtime interface {
	Kill(ctx context.Context, sandboxID string) error
}

// Nothing is a Runtime that kills nothing and remembers what it was asked.
type Nothing struct{ Killed []string }

func (n *Nothing) Kill(_ context.Context, sandboxID string) error {
	n.Killed = append(n.Killed, sandboxID)
	return nil
}

type Config struct {
	Addr    string
	Rules   *RuleSet
	Tiers   map[string]string // tenant to tier
	Runtime Runtime
	Sink    Sink      // may be nil: nothing is stored
	Record  io.Writer // may be nil: nothing is recorded. See recorder.go.
	Clock   clock.Clock
}

const (
	// Heartbeats arrive about once a second. Comparing a duration with half
	// a beat of slack stops a millisecond of jitter from deciding whether a
	// rule fires now or a second later.
	slack        = 500 * time.Millisecond
	adoptFor     = 30 * time.Second // how long a policy taken over from a proxy is kept
	killTimeout  = 5 * time.Second
	retryKill    = 10 * time.Second // how often a kill that did not take is tried again
	forgetAfter  = time.Minute      // a sandbox this long silent, with nothing held against it, is forgotten
	maxSandboxes = 10000            // sandboxes tracked at once. More are refused, not remembered.
	maxPending   = 20000            // rows waiting for the store
	keepRecent   = 1000             // decisions kept in memory for the timeline
)

// What a decision's note can say when the effect is not simply the action.
const (
	NoteShadow      = "shadow"             // the rule is in shadow mode, nothing changed
	NoteApproval    = "approval_requested" // paid tenant: capped at block_egress, a human decides the kill
	NoteBlastRadius = "blast_radius_cap"   // too many kills this minute: capped at block_egress, a human is needed
	NoteLifted      = "lifted"             // the action expired
	NoteAdopted     = "adopted"            // taken over from a proxy after a guard restart
)

// Decision is one row of the audit trail: a rule fired, or an action ended.
type Decision struct {
	Seq       uint64             `json:"seq"`
	At        time.Time          `json:"at"`
	TenantID  string             `json:"tenant_id"`
	SandboxID string             `json:"sandbox_id"`
	Tier      string             `json:"tier"`
	Rule      string             `json:"rule"`
	Mode      string             `json:"mode"`
	Action    string             `json:"action"`  // what the rule asked for
	Effect    string             `json:"effect"`  // what was done. Empty means nothing.
	Note      string             `json:"note"`    // why the two differ
	Rate      float64            `json:"rate"`    // throttle only
	Inputs    map[string]float64 `json:"inputs"`  // the rule variables at that moment
	Expires   time.Time          `json:"expires"` // when the effect ends unless renewed
}

// Usage is the counters of one sandbox at one moment, for the usage table.
type Usage struct {
	At        time.Time
	TenantID  string
	SandboxID string
	Started   time.Time // of the proxy: the run of counters these belong to
	Counters  control.Counters
}

// ErrFull means the guard cannot track one more sandbox.
var ErrFull = errors.New("guard is tracking as many sandboxes as it can")

type Guard struct {
	cfg Config

	mu        sync.Mutex // guards everything below
	tenants   map[string]*tenant
	sandboxes map[string]*sandbox
	order     []*sandbox  // every tracked sandbox, in order of first heartbeat
	kills     []time.Time // kills in the last minute, for the blast radius cap
	seq       uint64
	fresh     []Decision // made by the call in progress
	recent    []Decision // the last keepRecent, for the timeline
	actions   []Decision // waiting for the store
	usage     []Usage    // waiting for the store
	lost      int        // rows dropped because the store fell behind
	recorded  int        // bytes written to the recording
}

type tenant struct {
	tier    string
	members []*sandbox
}

type sandbox struct {
	id, tenantID string
	tenant       *tenant
	window       window
	baseline     baseline
	since        map[string]time.Time // rule name to when its expression became true
	held         map[string]*held     // rule name to the action it holds
	firing       bool                 // some rule's expression was true at the last heartbeat
	killed       bool
	killTried    time.Time // when the runtime was last asked to kill it
}

// held is one rule's action on one sandbox.
type held struct {
	policy  control.Policy // what it adds to the sandbox's policy. Zero for observe and shadow.
	expires time.Time
	refused bool // a kill the blast radius cap turned into block_egress, tried again while the rule fires
}

func New(cfg Config) *Guard {
	return &Guard{cfg: cfg, tenants: map[string]*tenant{}, sandboxes: map[string]*sandbox{}}
}

// Heartbeat takes one report from a proxy, judges the sandbox and returns
// the policy the proxy must enforce from now on.
func (g *Guard) Heartbeat(hb control.Heartbeat) (control.Policy, error) {
	now := g.cfg.Clock.Now()
	g.mu.Lock()
	s := g.sandboxes[hb.SandboxID]
	if s == nil {
		if len(g.order) >= maxSandboxes {
			g.mu.Unlock()
			return control.Policy{}, ErrFull
		}
		s = g.admit(hb, now)
	}
	s.window.add(now, hb.Counters)
	g.tape(now, hb)
	if len(g.usage) < maxPending {
		g.usage = append(g.usage, Usage{At: now, TenantID: s.tenantID, SandboxID: s.id, Started: hb.Started, Counters: hb.Counters})
	} else {
		g.lost++
	}
	// A heartbeat from a killed sandbox means its proxy is still alive: the
	// kill did not take, so it is tried again now and then.
	again := s.killed && now.Sub(s.killTried) >= retryKill
	if again {
		s.killTried = now
	} else if !s.killed {
		g.evaluate(s, now)
	}
	policy, fresh := s.policy(), g.fresh
	g.fresh = nil
	g.mu.Unlock()
	g.act(fresh) // outside the lock: a kill is a slow call
	if again {
		g.kill(s.id, slog.With("tenant_id", s.tenantID, "sandbox_id", s.id))
	}
	return policy, nil
}

// admit starts tracking a sandbox on its first heartbeat.
func (g *Guard) admit(hb control.Heartbeat, now time.Time) *sandbox {
	t := g.tenants[hb.TenantID]
	if t == nil {
		tier, known := g.cfg.Tiers[hb.TenantID]
		if !known {
			tier = scenario.TierPaid // automation never kills what it cannot place
		}
		t = &tenant{tier: tier}
		g.tenants[hb.TenantID] = t
	}
	s := &sandbox{id: hb.SandboxID, tenantID: hb.TenantID, tenant: t, since: map[string]time.Time{}, held: map[string]*held{}}
	t.members = append(t.members, s)
	g.sandboxes[s.id] = s
	g.order = append(g.order, s)
	if hb.Applied != (control.Policy{}) {
		// The proxy is enforcing something this guard has no record of, so
		// the guard restarted. Keep it for a while: a restart must never
		// loosen a limit. If the cause is still there a rule takes over.
		// Until the rules have judged the sandbox it counts as firing: what
		// it does now may be what it was contained for, and that is no
		// baseline to learn.
		s.firing = true
		s.held[NoteAdopted] = &held{policy: hb.Applied, expires: now.Add(adoptFor)}
		g.record(s, Decision{Rule: NoteAdopted, Mode: Enforce, Note: NoteAdopted, Rate: hb.Applied.EventRate, Expires: now.Add(adoptFor)}, now)
	}
	return s
}

// evaluate runs every rule against one sandbox.
func (g *Guard) evaluate(s *sandbox, now time.Time) {
	r, ok := s.window.rates()
	if !ok {
		return
	}
	// The tenant's view: a tenant can spread load over many sandboxes that
	// each look harmless. Its baseline is the sum of its sandboxes' own, so
	// a sandbox that starts or leaves moves the rate and the baseline alike.
	t := s.tenant
	tenantRate, tenantBase, reporting, othersFiring := 0.0, 0.0, 0.0, false
	for _, m := range t.members {
		if m.killed || now.Sub(m.window.last()) > staleAfter {
			continue
		}
		if mr, ok := m.window.rates(); ok {
			tenantRate += mr.offered
			reporting++
		}
		if m != s {
			tenantBase += m.baseline.value
			othersFiring = othersFiring || m.firing
		}
	}
	// A baseline takes in only what no rule objects to, anywhere in the
	// tenant. Otherwise a flood that lasts long enough becomes the new
	// normal and the rule that caught it goes quiet. The first window counts
	// as normal on the same condition. A sandbox without a baseline has
	// baseline zero, so only the fixed part of a rule can speak for it.
	if s.baseline.at.IsZero() && !s.firing && !othersFiring {
		s.baseline.learn(r.offered, now)
	}
	vars := map[string]float64{
		"offered_rate": r.offered, "accepted_rate": r.accepted, "reject_ratio": r.rejectRatio,
		"egress_bytes_rate": r.egressBytes, "watched_bytes_rate": r.watchedBytes, "bad_site_attempts": r.badSite,
		"baseline_rate": s.baseline.value, "tenant_offered_rate": tenantRate,
		"tenant_baseline_rate": tenantBase + s.baseline.value, "tenant_sandboxes": reporting,
	}
	inputs := map[string]any{"tier": t.tier}
	for name, v := range vars {
		inputs[name] = v
	}

	s.firing = false
	for _, rule := range g.cfg.Rules.Rules {
		if !rule.holds(inputs) {
			delete(s.since, rule.Name)
			continue
		}
		s.firing = true
		if _, running := s.since[rule.Name]; !running {
			s.since[rule.Name] = now
		}
		if now.Sub(s.since[rule.Name])+slack >= rule.For {
			g.fire(s, rule, vars, now)
		}
		if s.killed {
			return // final: no later rule gets to kill it a second time
		}
	}
	if !s.firing && !othersFiring {
		s.baseline.learn(r.offered, now)
	}
}

// fire applies a rule whose expression has held long enough.
func (g *Guard) fire(s *sandbox, rule *Rule, vars map[string]float64, now time.Time) {
	expires := now.Add(rule.TTL)
	h := s.held[rule.Name]
	if h != nil && !h.refused {
		h.expires = expires // already in force: the rule still fires, so it stays
		return
	}
	d := Decision{Rule: rule.Name, Mode: rule.Mode, Action: rule.Action, Effect: rule.Action, Inputs: vars, Expires: expires}
	if rule.Rate != nil {
		d.Rate = *rule.Rate
	}
	// A kill that automation may not carry out becomes block_egress and a
	// call for a human: the outbound calls stop, the tenant's events are kept.
	switch {
	case rule.Mode == Shadow:
		d.Effect, d.Note = "", NoteShadow
	case rule.Action == Kill && s.tenant.tier != scenario.TierFree:
		d.Effect, d.Note = BlockEgress, NoteApproval // never a paid tenant's sandbox
	case rule.Action == Kill && !g.mayKill(now):
		d.Effect, d.Note = BlockEgress, NoteBlastRadius
	}
	if h != nil && d.Note == NoteBlastRadius {
		h.expires = expires // still over the cap. It was said once, that is enough.
		return
	}
	h = &held{expires: expires, refused: d.Note == NoteBlastRadius}
	switch d.Effect {
	case Throttle:
		h.policy = control.Policy{Throttle: true, EventRate: d.Rate}
	case BlockEgress:
		h.policy = control.Policy{BlockEgress: true}
	case Kill:
		s.killed, s.killTried = true, now
		g.kills = append(g.kills, now)
	}
	s.held[rule.Name] = h
	g.record(s, d, now)
}

// mayKill is the blast radius cap: at most MaxKillsPerMinute kills in any
// minute. A bad rule can then take out a few sandboxes, not all of them.
func (g *Guard) mayKill(now time.Time) bool {
	g.kills = slices.DeleteFunc(g.kills, func(at time.Time) bool { return now.Sub(at) >= time.Minute })
	return len(g.kills) < g.cfg.Rules.MaxKillsPerMinute
}

// policy is everything held against a sandbox, merged. Events and egress
// are separate controls, and on each one the strictest action wins.
func (s *sandbox) policy() control.Policy {
	if s.killed {
		// Also the answer if the kill itself failed: the sandbox stays
		// contained by its proxy and by ingest.
		return control.CutOff
	}
	var p control.Policy
	for _, h := range s.held {
		p = p.Merge(h.policy)
	}
	return p
}

// Sweep lifts every action whose time is up and forgets the sandboxes that
// are gone. It runs on a timer and not on heartbeats, because a sandbox that
// went silent must be released too.
func (g *Guard) Sweep() {
	now := g.cfg.Clock.Now()
	g.mu.Lock()
	for _, s := range g.order {
		// Sorted, so the same input always gives the same audit trail.
		for _, name := range slices.Sorted(maps.Keys(s.held)) {
			h := s.held[name]
			if now.Before(h.expires) {
				continue
			}
			delete(s.held, name)
			if h.policy != (control.Policy{}) && !s.killed {
				g.record(s, Decision{Rule: name, Mode: Enforce, Note: NoteLifted, Expires: now}, now)
			}
		}
	}
	// Without this the guard would fill up with sandboxes that no longer
	// exist. One that comes back is admitted again like a new one.
	g.order = slices.DeleteFunc(g.order, func(s *sandbox) bool {
		if len(s.held) > 0 || now.Sub(s.window.last()) < forgetAfter {
			return false
		}
		delete(g.sandboxes, s.id)
		t := s.tenant
		if t.members = slices.DeleteFunc(t.members, func(m *sandbox) bool { return m == s }); len(t.members) == 0 {
			delete(g.tenants, s.tenantID)
		}
		return true
	})
	fresh := g.fresh
	g.fresh = nil
	g.mu.Unlock()
	g.act(fresh)
}

// record adds a decision to the audit trail. The caller holds mu.
func (g *Guard) record(s *sandbox, d Decision, now time.Time) {
	g.seq++
	d.Seq, d.At, d.TenantID, d.SandboxID, d.Tier = g.seq, now, s.tenantID, s.id, s.tenant.tier
	g.fresh = append(g.fresh, d)
	g.recent = append(g.recent, d)
	if len(g.recent) > keepRecent {
		g.recent = g.recent[1:]
	}
	if len(g.actions) < maxPending {
		g.actions = append(g.actions, d)
	} else {
		g.lost++
	}
}

// act does what a decision needs outside the guard's memory: a log line,
// and for a kill the call to the runtime.
func (g *Guard) act(fresh []Decision) {
	for _, d := range fresh {
		log := slog.With("tenant_id", d.TenantID, "sandbox_id", d.SandboxID, "rule", d.Rule,
			"action", d.Action, "effect", d.Effect, "note", d.Note, "mode", d.Mode)
		switch d.Note {
		case NoteApproval:
			log.Warn("approval requested: a rule asked to kill a paid tenant's sandbox")
		case NoteBlastRadius:
			log.Error("blast radius cap reached, kill refused and egress blocked: a human is needed")
		default:
			log.Info("decision")
		}
		if d.Effect == Kill {
			g.kill(d.SandboxID, log)
		}
	}
}

// kill asks the runtime to stop a sandbox and its proxy.
func (g *Guard) kill(sandboxID string, log *slog.Logger) {
	// Not the heartbeat's context: a proxy that hangs up must not cancel its own kill.
	ctx, cancel := context.WithTimeout(context.Background(), killTimeout)
	defer cancel()
	if err := g.cfg.Runtime.Kill(ctx, sandboxID); err != nil {
		log.Error("kill failed, the sandbox stays cut off by its proxy and by ingest", "err", err)
	}
}

// Denied lists the sandboxes whose events ingest must drop: the killed ones
// and the ones throttled to zero.
func (g *Guard) Denied() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := []string{}
	for _, s := range g.order {
		if s.policy().NoEvents() {
			ids = append(ids, s.id)
		}
	}
	return ids
}

// Since returns the remembered decisions made after number seq.
func (g *Guard) Since(seq uint64) []Decision {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := []Decision{}
	for _, d := range g.recent {
		if d.Seq > seq {
			out = append(out, d)
		}
	}
	return out
}
