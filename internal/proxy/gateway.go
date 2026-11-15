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

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
)

// Accepted is the reply to a batch that was at least partly accepted. A
// producer can see exactly which events were refused and why.
type Accepted struct {
	Accepted int           `json:"accepted"`
	Rejected []RejectedRow `json:"rejected,omitempty"`
}

type RejectedRow struct {
	Index  int          `json:"index"`
	Reason event.Reason `json:"reason"`
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
	var reply Accepted
	var good []event.Event
	var size uint64
	for i, e := range in.Events {
		if reason := event.Check(e); reason != "" {
			reply.Rejected = append(reply.Rejected, RejectedRow{Index: i, Reason: reason})
			p.count(func(c *Counters) { c.Rejects[reason]++ })
			continue
		}
		event.Stamp(&e, p.cfg.TenantID, p.cfg.SandboxID, now)
		good = append(good, e)
		size += uint64(len(e.Payload))
	}
	if len(good) > 0 {
		if ok, wait := p.bucket.take(len(good)); !ok {
			p.refuseEvents(w, http.StatusTooManyRequests, event.RateLimited, len(good), wait)
			return
		}
		// The buffer toward ingest is full: push back, do not grow.
		if !p.queue.TryPush(good) {
			p.refuseEvents(w, http.StatusTooManyRequests, event.BufferFull, len(good), time.Second)
			return
		}
		p.count(func(c *Counters) { c.EventsAccepted += uint64(len(good)); c.BytesAccepted += size })
	}
	reply.Accepted = len(good)
	httpx.WriteJSON(w, http.StatusOK, reply)
}

// refuseEvents refuses a whole request and counts n rejections for it.
func (p *Proxy) refuseEvents(w http.ResponseWriter, status int, reason event.Reason, n int, retryAfter time.Duration) {
	p.count(func(c *Counters) { c.Rejects[reason] += uint64(n) })
	httpx.Refuse(w, status, reason, retryAfter)
}

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
		}
		batch := p.queue.Peek(p.cfg.ForwardBatch)
		if len(batch) == 0 {
			continue
		}
		if err := p.send(ctx, batch); err != nil {
			slog.Warn("forward to ingest failed, will retry", "err", err, "events", len(batch))
			continue
		}
		p.queue.Drop(len(batch))
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
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("ingest replied %s", resp.Status)
	}
	return nil
}
