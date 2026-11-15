// Package event defines the event schema, the checks every event must pass,
// and the reason codes returned when something is refused.
package event

import (
	"crypto/rand"
	"fmt"
	"time"
)

// Reason is a machine-readable code sent with every refusal, so a
// well-behaved producer can fix itself instead of retrying forever.
type Reason string

const (
	Malformed     Reason = "malformed"       // body is not valid JSON
	BodyTooLarge  Reason = "body_too_large"  // request body over the byte limit
	BatchTooLarge Reason = "batch_too_large" // more events than one request may carry
	UnknownType   Reason = "unknown_type"    // event type is not in the schema
	Oversized     Reason = "oversized_event" // one event's payload is over the limit
	RateLimited   Reason = "rate_limited"    // token bucket is empty
	BufferFull    Reason = "buffer_full"     // downstream is slow, try again later
	SiteBad       Reason = "site_bad"        // egress to a known bad domain
	SiteUnlisted  Reason = "site_unlisted"   // egress to a domain in no site class
	ForbiddenPeer Reason = "forbidden_peer"  // caller is not this proxy's sandbox
	UpstreamError Reason = "upstream_error"  // the remote host could not be reached
	EgressBlocked Reason = "egress_blocked"  // the guard has blocked this sandbox's egress
	GuardFull     Reason = "guard_full"      // the guard tracks as many sandboxes as it can
)

const (
	MaxPayloadBytes = 4096
	// MaxSkew bounds event_time. The table is partitioned by day, so without
	// this a sandbox could create a new partition for every date it invents.
	MaxSkew = time.Hour
)

var knownTypes = map[string]bool{"log": true, "metric": true, "span": true}

// Event is one record from a sandbox. The struct tags give the JSON names.
type Event struct {
	EventID   string    `json:"event_id"`
	TenantID  string    `json:"tenant_id"`
	SandboxID string    `json:"sandbox_id"`
	Type      string    `json:"type"`
	EventTime time.Time `json:"event_time"`
	Payload   string    `json:"payload"`
}

// Batch is the wire format between sandbox, proxy and ingest.
type Batch struct {
	Events []Event `json:"events"`
}

// Rejected names one refused event in a batch, by its position.
type Rejected struct {
	Index  int    `json:"index"`
	Reason Reason `json:"reason"`
}

// Size is roughly the memory one event holds: its strings plus a fixed
// overhead for the struct. Queues are bounded by it.
func (e Event) Size() int {
	return 128 + len(e.EventID) + len(e.TenantID) + len(e.SandboxID) + len(e.Type) + len(e.Payload)
}

// Check returns the reason an event must be refused, or "" if it is fine.
func Check(e Event) Reason {
	if !knownTypes[e.Type] {
		return UnknownType
	}
	if len(e.Payload) > MaxPayloadBytes {
		return Oversized
	}
	return ""
}

// Stamped reports whether an event looks like it went through Stamp. Ingest
// refuses anything else: an event the store can never take would otherwise
// sit at the head of the shared queue and block every tenant.
func Stamped(e Event) bool {
	return Check(e) == "" && e.TenantID != "" && e.SandboxID != "" && !e.EventTime.IsZero() && validID(e.EventID)
}

// validID checks the UUID layout, which is what the store's column needs.
func validID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Stamp replaces everything the sandbox must not control. Whatever identity
// or ID the sandbox sent is discarded, and its timestamp is clamped.
func Stamp(e *Event, tenantID, sandboxID string, now time.Time) {
	e.TenantID, e.SandboxID = tenantID, sandboxID
	e.EventID = NewID(now)
	e.EventTime = clamp(e.EventTime, now)
}

func clamp(t, now time.Time) time.Time {
	switch {
	case t.IsZero():
		return now
	case t.Before(now.Add(-MaxSkew)):
		return now.Add(-MaxSkew)
	case t.After(now.Add(MaxSkew)):
		return now.Add(MaxSkew)
	}
	return t.UTC()
}

// NewID returns a UUIDv7 (RFC 9562): 48 bits of milliseconds, then random
// bits. IDs sort by creation time, which keeps inserts into ClickHouse
// roughly ordered. Written by hand because the layout is a few lines and
// the standard library has no UUID type.
func NewID(now time.Time) string {
	var b [16]byte
	rand.Read(b[:]) // never fails: crypto/rand aborts the program if the OS has no entropy
	ms := uint64(now.UnixMilli())
	b[0], b[1], b[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	b[3], b[4], b[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = b[6]&0x0f | 0x70 // version 7
	b[8] = b[8]&0x3f | 0x80 // RFC variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
