// Package clock lets code read the time through an interface, so tests and
// replay can decide what time it is.
package clock

import (
	"sync"
	"time"
)

// Clock is the only way production code reads the time.
// Go idiom: an interface is satisfied by any type that has these methods,
// with no "implements" declaration.
type Clock interface {
	Now() time.Time
}

// Real reads the system clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

// Fake is a clock that only moves when a test tells it to.
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{t: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	// Go idiom: defer runs this when the function returns, like a finally block.
	defer f.mu.Unlock()
	return f.t
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}
