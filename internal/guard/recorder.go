package guard

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// A recording is a text file with one JSON value per line. The first line
// is a Header, every other line a Record. Replaying the records through a
// guard, with the clock set to each record's time, shows what any rule set
// would have decided about the same traffic.

// Header is the scenario the recording was made from. It is the ground
// truth a replay is scored against: who misbehaves, how, and from when.
type Header struct {
	// Enforced means rules were acting while this was recorded, so the
	// traffic already shows what one rule set did to it. To compare rule
	// sets fairly, record with every rule in shadow.
	Enforced bool              `json:"enforced"`
	Tenants  []scenario.Tenant `json:"tenants"`
}

// Record is one heartbeat and when it arrived.
// Go idiom: a struct named without a field name is embedded. Its fields
// become fields of Record, in Go and in the JSON.
type Record struct {
	At time.Time `json:"at"`
	control.Heartbeat
}

const (
	// MaxRecording bounds a recording in bytes. A forgotten recorder must
	// not fill the disk.
	MaxRecording = 64 << 20
	// recordQueue is how many heartbeats may wait for the disk: about a
	// second's worth at the most sandboxes the guard tracks.
	recordQueue = maxSandboxes
)

// Recorder writes a recording from a goroutine of its own, so the guard
// never waits on the disk. A recording with a gap would score rule sets on
// traffic that never happened. So when a write fails, or the disk falls so
// far behind that a heartbeat would have to wait, the recording stops and
// Close says so. One that reaches MaxRecording stops too, but what it has
// is whole.
type Recorder struct {
	queue chan Record
	done  chan struct{} // closed when everything queued is written
	size  int           // bytes written. Only the writing goroutine touches it.

	mu     sync.Mutex // guards the two below
	err    error      // why the recording has a gap. Nil while it has none.
	closed bool
}

// NewRecorder writes the header and starts the goroutine that writes the
// records.
func NewRecorder(w io.Writer, h Header) (*Recorder, error) {
	buf := bufio.NewWriterSize(w, 64<<10)
	if err := json.NewEncoder(buf).Encode(h); err != nil {
		return nil, err
	}
	if err := buf.Flush(); err != nil {
		return nil, err
	}
	r := &Recorder{queue: make(chan Record, recordQueue), done: make(chan struct{})}
	go r.write(buf)
	return r, nil
}

// add queues one heartbeat. The guard calls it with its lock held, so the
// queue is in the order the guard judged the heartbeats.
func (r *Recorder) add(rec Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.err != nil {
		return
	}
	// Go idiom: a select with a default never blocks. If the queue is full,
	// the default runs instead of waiting for room.
	select {
	case r.queue <- rec:
	default:
		r.stop(errors.New("the disk fell behind and a heartbeat was not recorded"))
	}
}

// write marshals and writes what add queued. It flushes whenever the queue
// is empty: few writes while the guard is busy, a file that is up to date
// while it is not.
func (r *Recorder) write(w *bufio.Writer) {
	defer close(r.done)
	// Go idiom: ranging over a channel takes values until it is closed and
	// empty.
	for rec := range r.queue {
		if r.size >= MaxRecording || r.failed() {
			continue // stopped, but still draining, so add never finds the queue full
		}
		line, err := json.Marshal(rec)
		if err == nil {
			var n int
			n, err = w.Write(append(line, '\n'))
			r.size += n
		}
		if err == nil && (len(r.queue) == 0 || r.size >= MaxRecording) {
			err = w.Flush()
		}
		switch {
		case err != nil:
			r.mu.Lock()
			r.stop(err)
			r.mu.Unlock()
		case r.size >= MaxRecording:
			slog.Error("recording stopped at its size limit, what it has is whole", "bytes", r.size)
		}
	}
}

func (r *Recorder) failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err != nil
}

// stop ends a recording that has a gap. The caller holds mu.
func (r *Recorder) stop(err error) {
	if r.err == nil {
		r.err = err
		slog.Error("recording stopped, it has a gap", "err", err)
	}
}

// Close waits until everything queued is written. An error means the
// recording has a gap and must not be used.
func (r *Recorder) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.queue)
	}
	r.mu.Unlock()
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}
