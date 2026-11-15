package guard

import (
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

const (
	// windowLen heartbeats make one window. At one heartbeat a second that
	// is two seconds: long enough to smooth one odd second, short enough to
	// contain a flood within a few.
	windowLen = 3
	// A sandbox that has been silent this long is left out of its tenant's totals.
	staleAfter = 3 * time.Second
	// How slowly a baseline follows a lasting change: about two thirds of
	// the way after this long.
	baselineTau = 60 * time.Second
)

// snapshot is the counters of one heartbeat and when it arrived.
type snapshot struct {
	at time.Time
	c  control.Counters
}

// window is the last few snapshots of one sandbox. Counters are cumulative,
// so a rate is the difference between the newest and the oldest snapshot
// divided by the time between them. A late heartbeat stretches the window
// and does not distort the rate.
type window struct {
	snaps []snapshot
}

func (w *window) add(at time.Time, c control.Counters) {
	w.snaps = append(w.snaps, snapshot{at, c})
	if len(w.snaps) > windowLen {
		w.snaps = w.snaps[1:]
	}
}

// last is when the newest snapshot arrived.
func (w *window) last() time.Time { return w.snaps[len(w.snaps)-1].at }

// rates is what one window says about one sandbox.
type rates struct {
	offered      float64 // events per second the sandbox tried to send
	accepted     float64 // events per second that got through
	rejectRatio  float64 // share of offered events that were refused
	egressBytes  float64 // bytes per second to allowed sites, all classes
	watchedBytes float64 // bytes per second to watched sites
	badSite      float64 // calls to bad sites in the window, a count
}

// rates returns false until the window is full, so a sandbox is not judged
// on its first second.
func (w *window) rates() (r rates, ok bool) {
	if len(w.snaps) < windowLen {
		return r, false
	}
	old, cur := w.snaps[0].c, w.snaps[len(w.snaps)-1].c
	secs := w.last().Sub(w.snaps[0].at).Seconds()
	if secs <= 0 {
		return r, false
	}
	refused := delta(rejected(cur), rejected(old)) / secs
	r.accepted = delta(cur.EventsAccepted, old.EventsAccepted) / secs
	r.offered = r.accepted + refused
	if r.offered > 0 {
		r.rejectRatio = refused / r.offered
	}
	r.egressBytes = delta(control.Total(cur.EgressByClass), control.Total(old.EgressByClass)) / secs
	r.watchedBytes = delta(cur.EgressByClass[scenario.ClassWatched], old.EgressByClass[scenario.ClassWatched]) / secs
	r.badSite = delta(cur.Blocked[event.SiteBad], old.Blocked[event.SiteBad])
	return r, true
}

// rejected counts the refusals that are the sandbox's own doing. buffer_full
// is ours: a slow ingest must not make every tenant look like an attacker.
func rejected(c control.Counters) uint64 {
	return control.Total(c.Rejects) - c.Rejects[event.BufferFull]
}

// delta is how much a counter grew. A counter that went down means the proxy
// restarted. That reads as no growth, not as a huge number.
func delta(cur, old uint64) float64 {
	if cur < old {
		return 0
	}
	return float64(cur - old)
}

// baseline is a slow moving average of a rate: what this sandbox or tenant
// normally does. A rule can then ask whether it is behaving unlike itself.
type baseline struct {
	value float64
	at    time.Time // zero until the first value
}

// learn moves the baseline a little toward x. The first value is taken as is.
func (b *baseline) learn(x float64, now time.Time) {
	if b.at.IsZero() {
		b.value = x
	} else {
		b.value += (x - b.value) * min(1, now.Sub(b.at).Seconds()/baselineTau.Seconds())
	}
	b.at = now
}
