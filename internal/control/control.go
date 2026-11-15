// Package control is the protocol between the guard and what it steers: the
// heartbeat a proxy sends every second, the policy it gets back, and the
// deny list ingest fetches.
package control

import (
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
)

// Counters only ever go up. A reader that misses a heartbeat loses nothing,
// because the next one carries the totals.
type Counters struct {
	EventsAccepted uint64                  `json:"events_accepted"`
	BytesAccepted  uint64                  `json:"bytes_accepted"`
	Dropped        uint64                  `json:"dropped"`         // accepted events that ingest refused for good
	Rejects        map[event.Reason]uint64 `json:"rejects"`         // refused events and requests, by reason
	EgressBytes    map[string]uint64       `json:"egress_bytes"`    // bytes both ways, by domain
	EgressByClass  map[string]uint64       `json:"egress_by_class"` // the same bytes, by site class
	Blocked        map[event.Reason]uint64 `json:"blocked"`         // refused egress attempts, by reason
}

// Total adds up one of the maps in Counters.
// Go idiom: [K comparable] makes this one function work for any key type.
func Total[K comparable](m map[K]uint64) (n uint64) {
	for _, v := range m {
		n += v
	}
	return n
}

// Policy is what the guard asks one proxy to enforce on top of its static
// limits. Events and egress are separate controls. The zero value asks for
// nothing extra.
type Policy struct {
	Throttle    bool    `json:"throttle"`     // EventRate replaces the static rate
	EventRate   float64 `json:"event_rate"`   // events per second. Zero is the cut-off.
	BlockEgress bool    `json:"block_egress"` // refuse every outbound call
}

// CutOff is the policy of a killed sandbox: nothing in, nothing out.
var CutOff = Policy{Throttle: true, EventRate: 0, BlockEgress: true}

// Merge returns the stricter of two policies on each control.
func (p Policy) Merge(q Policy) Policy {
	if q.Throttle && (!p.Throttle || q.EventRate < p.EventRate) {
		p.Throttle, p.EventRate = true, q.EventRate
	}
	p.BlockEgress = p.BlockEgress || q.BlockEgress
	return p
}

// NoEvents reports whether the policy lets no event through at all. Ingest
// drops the events of such a sandbox.
func (p Policy) NoEvents() bool { return p.Throttle && p.EventRate <= 0 }

// Heartbeat is what a proxy reports. Applied is the policy it enforces right
// now, so a guard that restarted with an empty memory can take it over.
// Started is when the proxy began counting from zero: it tells one run of
// counters from the next, so usage survives a proxy restart.
type Heartbeat struct {
	TenantID  string    `json:"tenant_id"`
	SandboxID string    `json:"sandbox_id"`
	Started   time.Time `json:"started"`
	Counters  Counters  `json:"counters"`
	Applied   Policy    `json:"applied"`
}

// DenyList names the sandboxes whose events ingest must drop.
type DenyList struct {
	Sandboxes []string `json:"sandboxes"`
}
