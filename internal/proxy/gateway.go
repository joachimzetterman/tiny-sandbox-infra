package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
)

// Accepted is the reply to a batch that was at least partly accepted. A
// producer can see exactly which events were refused and why.
type Accepted struct {
	Accepted int              `json:"accepted"`
	Rejected []event.Rejected `json:"rejected,omitempty"`
}

// handleEvents is the event gateway. Order matters: check and stamp first,
// then rate limit, then buffer. Bad events never spend tokens, and nothing
// is buffered that was not paid for.
func (p *Proxy) handleEvents(w http.ResponseWriter, r *http.Request) {
	var in event.Batch
	body := http.MaxBytesReader(w, r.Body, maxEventBody)
	if err := json.NewDecoder(body).Decode(&in); err != nil {
		var tooBig *http.MaxBytesError
		// Go idiom: errors.As asks "is this error, or one it wraps, of this type?"
		if errors.As(err, &tooBig) {
			p.refuseEvents(w, http.StatusRequestEntityTooLarge, event.BodyTooLarge, 1, 0)
			return
		}
		p.refuseEvents(w, http.StatusBadRequest, event.Malformed, 1, 0)
		return
	}
	// A batch larger than the bucket could never be accepted, so say that
	// instead of "rate limited", which would invite a retry.
	if len(in.Events) > p.bucket.size() {
		p.refuseEvents(w, http.StatusRequestEntityTooLarge, event.BatchTooLarge, len(in.Events), 0)
		return
	}

	now := p.cfg.Clock.Now()
	var good []event.Event
	var bad []event.Rejected
	var size uint64
	for i, e := range in.Events {
		if reason := event.Check(e); reason != "" {
			bad = append(bad, event.Rejected{Index: i, Reason: reason})
			continue
		}
		event.Stamp(&e, p.cfg.TenantID, p.cfg.SandboxID, now)
		good = append(good, e)
		size += uint64(len(e.Payload))
	}
	if len(good) > 0 {
		if ok, wait := p.bucket.take(len(good)); !ok {
			p.refuseEvents(w, http.StatusTooManyRequests, event.RateLimited, len(good), wait, bad...)
			return
		}
		// The buffer toward ingest is full: push back, do not grow.
		if !p.queue.TryPush(good) {
			p.bucket.refund(len(good)) // nothing was accepted, so nothing is spent
			p.refuseEvents(w, http.StatusTooManyRequests, event.BufferFull, len(good), time.Second, bad...)
			return
		}
	}
	// Bad events are counted only here, once the request has its final
	// answer. A 429 is retried, and counting on every attempt would inflate
	// the numbers the guard reads.
	p.count(func(c *control.Counters) {
		c.EventsAccepted += uint64(len(good))
		c.BytesAccepted += size
		for _, r := range bad {
			c.Rejects[r.Reason]++
		}
	})
	httpx.WriteJSON(w, http.StatusOK, Accepted{Accepted: len(good), Rejected: bad})
}

// refuseEvents refuses a whole request and counts n rejections for it. bad
// lists the events that would be refused anyway, so the producer can fix
// them before it retries.
func (p *Proxy) refuseEvents(w http.ResponseWriter, status int, reason event.Reason, n int, retryAfter time.Duration, bad ...event.Rejected) {
	p.count(func(c *control.Counters) { c.Rejects[reason] += uint64(n) })
	httpx.Refuse(w, status, reason, retryAfter, bad...)
}

// errRefused means ingest will never take this batch, however often it is sent.
var errRefused = errors.New("ingest refused the batch for good")

// forwardLoop drains the buffer toward ingest. A batch leaves the buffer
// only after ingest took it, so a slow or failing ingest makes the buffer
// fill up and the gateway answer 429. Delivery is at least once.
func (p *Proxy) forwardLoop(ctx context.Context) {
	tick := time.NewTicker(p.cfg.ForwardEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.forwardOnce(ctx)
		}
	}
}

// forwardOnce sends the oldest batch and reports whether the buffer moved.
func (p *Proxy) forwardOnce(ctx context.Context) bool {
	batch := p.queue.Peek(p.cfg.ForwardBatch)
	if len(batch) == 0 {
		return false
	}
	err := p.send(ctx, batch)
	if errors.Is(err, errRefused) {
		// Retrying would block everything behind this batch forever. Losing
		// it is the lesser harm, and it is counted and logged loudly.
		slog.Error("ingest refused a batch for good, dropping it", "events", len(batch))
		p.count(func(c *control.Counters) { c.Dropped += uint64(len(batch)) })
	} else if err != nil {
		slog.Warn("forward to ingest failed, will retry", "err", err, "events", len(batch))
		return false
	}
	p.queue.Drop(len(batch))
	return true
}

// drain hands over what is still buffered when the proxy is asked to stop.
// It gives up on the first failure or when the time is up.
func (p *Proxy) drain() {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	for p.forwardOnce(ctx) {
	}
}

func (p *Proxy) send(ctx context.Context, batch []event.Event) error {
	body, err := json.Marshal(event.Batch{Events: batch})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.IngestURL+"/v1/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := p.ingest.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests:
		return errRefused // 429 means "later", every other 4xx means "never"
	}
	return fmt.Errorf("ingest replied %s", resp.Status)
}
