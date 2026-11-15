package event

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestCheck(t *testing.T) {
	cases := []struct {
		name string
		e    Event
		want Reason
	}{
		{"known type", Event{Type: "log", Payload: "x"}, ""},
		{"unknown type", Event{Type: "exploit"}, UnknownType},
		{"empty type", Event{}, UnknownType},
		{"payload at the limit", Event{Type: "log", Payload: strings.Repeat("x", MaxPayloadBytes)}, ""},
		{"payload over the limit", Event{Type: "log", Payload: strings.Repeat("x", MaxPayloadBytes+1)}, Oversized},
	}
	for _, c := range cases {
		if got := Check(c.e); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestStampDiscardsWhatTheSandboxClaimed(t *testing.T) {
	e := Event{EventID: "mine", TenantID: "victim", SandboxID: "other", Type: "log", EventTime: now}
	Stamp(&e, "t-real", "s-real", now)
	if e.TenantID != "t-real" || e.SandboxID != "s-real" {
		t.Errorf("identity not overwritten: %+v", e)
	}
	if e.EventID == "mine" {
		t.Error("event_id from the sandbox was kept")
	}
}

func TestStampClampsEventTime(t *testing.T) {
	cases := []struct {
		name     string
		in, want time.Time
	}{
		{"inside the window is kept", now.Add(-10 * time.Minute), now.Add(-10 * time.Minute)},
		{"far past is pulled to one hour ago", now.AddDate(-20, 0, 0), now.Add(-time.Hour)},
		{"far future is pulled to one hour ahead", now.AddDate(20, 0, 0), now.Add(time.Hour)},
		{"missing becomes now", time.Time{}, now},
	}
	for _, c := range cases {
		e := Event{Type: "log", EventTime: c.in}
		Stamp(&e, "t", "s", now)
		if !e.EventTime.Equal(c.want) {
			t.Errorf("%s: got %s, want %s", c.name, e.EventTime, c.want)
		}
	}
}

func TestNewIDIsUUIDv7AndSortsByTime(t *testing.T) {
	shape := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	early, late := NewID(now), NewID(now.Add(time.Millisecond))
	for _, id := range []string{early, late} {
		if !shape.MatchString(id) {
			t.Errorf("%q is not a UUIDv7", id)
		}
	}
	if early >= late {
		t.Errorf("IDs do not sort by time: %q then %q", early, late)
	}
	if a, b := NewID(now), NewID(now); a == b {
		t.Error("two IDs in the same millisecond are equal")
	}
}

func TestQueueIsBoundedAndAllOrNothing(t *testing.T) {
	q := NewQueue(3)
	if !q.TryPush(make([]Event, 2)) {
		t.Fatal("push within the limit was refused")
	}
	if q.TryPush(make([]Event, 2)) {
		t.Fatal("push over the limit was accepted")
	}
	if q.Len() != 2 {
		t.Fatalf("a refused push changed the queue: len %d", q.Len())
	}
	if got := q.Peek(10); len(got) != 2 || q.Len() != 2 {
		t.Fatalf("peek must not remove: got %d, len %d", len(got), q.Len())
	}
	q.Drop(2)
	if q.Len() != 0 || !q.TryPush(make([]Event, 3)) {
		t.Fatal("space was not freed by drop")
	}
}
