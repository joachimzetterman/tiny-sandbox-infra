package event

import "sync"

// Queue is a bounded FIFO of events. It never grows past its limits: a push
// that does not fit is refused, and the caller turns that into a 429.
//
// There are two limits. The byte limit is what protects memory, because an
// event can be 30 bytes or 4 KiB. The event limit keeps one flush a
// predictable size.
//
// Readers use Peek then Drop, so a batch only leaves the queue after it was
// delivered. A failed delivery is retried with the same events.
type Queue struct {
	mu        sync.Mutex // guards the rest: HTTP handlers and the flusher run concurrently
	items     []Event
	bytes     int // sum of Size over items
	maxEvents int
	maxBytes  int
}

func NewQueue(maxEvents, maxBytes int) *Queue {
	return &Queue{maxEvents: maxEvents, maxBytes: maxBytes}
}

// TryPush adds all the events or none of them.
func (q *Queue) TryPush(evs []Event) bool {
	size := 0
	for _, e := range evs {
		size += e.Size()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items)+len(evs) > q.maxEvents || q.bytes+size > q.maxBytes {
		return false
	}
	q.items = append(q.items, evs...)
	q.bytes += size
	return true
}

// Peek returns a copy of up to n of the oldest events without removing them.
func (q *Queue) Peek(n int) []Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	n = min(n, len(q.items))
	return append([]Event(nil), q.items[:n]...)
}

// Drop removes the n oldest events, after they were delivered.
func (q *Queue) Drop(n int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, e := range q.items[:n] {
		q.bytes -= e.Size()
	}
	// Copy the rest into a fresh slice so the delivered events can be freed.
	q.items = append([]Event(nil), q.items[n:]...)
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
