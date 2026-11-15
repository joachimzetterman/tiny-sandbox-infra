package event

import "sync"

// Queue is a bounded FIFO of events. It never grows past its limit: a push
// that does not fit is refused, and the caller turns that into a 429.
//
// Readers use Peek then Drop, so a batch only leaves the queue after it was
// delivered. A failed delivery is retried with the same events.
type Queue struct {
	mu    sync.Mutex // guards items: HTTP handlers and the flusher run concurrently
	items []Event
	limit int
}

func NewQueue(limit int) *Queue { return &Queue{limit: limit} }

// TryPush adds all the events or none of them.
func (q *Queue) TryPush(evs []Event) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items)+len(evs) > q.limit {
		return false
	}
	q.items = append(q.items, evs...)
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
	// Copy the rest into a fresh slice so the delivered events can be freed.
	q.items = append([]Event(nil), q.items[n:]...)
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
