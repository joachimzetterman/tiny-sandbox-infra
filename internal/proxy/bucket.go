package proxy

import (
	"sync"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
)

// maxRetryAfter caps the wait we tell a client, including when the rate is zero.
const maxRetryAfter = 60 * time.Second

// bucket is a token bucket: it refills at rate tokens per second up to
// burst, and each accepted event spends one token.
type bucket struct {
	mu     sync.Mutex
	clock  clock.Clock
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(c clock.Clock, rate float64, burst int) *bucket {
	return &bucket{clock: c, rate: rate, burst: float64(burst), tokens: float64(burst), last: c.Now()}
}

// take spends n tokens if they are available. If not, it spends nothing
// and returns how long until n tokens will be there.
func (b *bucket) take(n int) (ok bool, wait time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	need := float64(n) - b.tokens
	if need <= 0 {
		b.tokens -= float64(n)
		return true, 0
	}
	if b.rate <= 0 {
		return false, maxRetryAfter
	}
	return false, min(maxRetryAfter, time.Duration(need/b.rate*float64(time.Second)))
}

// size is the largest batch that can ever be accepted.
func (b *bucket) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int(b.burst)
}
