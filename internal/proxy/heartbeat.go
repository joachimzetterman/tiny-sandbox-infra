package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
)

// heartbeatLoop reports the counters to the guard. The reply carries the
// policy for this sandbox, so the proxy needs no inbound control port.
func (p *Proxy) heartbeatLoop(ctx context.Context) {
	tick := time.NewTicker(p.cfg.Heartbeat)
	defer tick.Stop()
	down := false // used to log once per outage, not once per second
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		policy, err := p.beat(ctx)
		if err != nil {
			// Fail closed: the last policy and the static limits stay in
			// force. A guard that is down never loosens anything.
			if !down {
				slog.Warn("guard unreachable, keeping the last policy", "err", err)
			}
			down = true
			continue
		}
		down = false
		p.apply(policy)
	}
}

// beat sends one heartbeat and returns the policy in the reply.
func (p *Proxy) beat(ctx context.Context) (policy control.Policy, err error) {
	body, err := json.Marshal(control.Heartbeat{
		TenantID: p.cfg.TenantID, SandboxID: p.cfg.SandboxID, Started: p.started, Counters: p.Counters(), Applied: p.Policy(),
	})
	if err != nil {
		return policy, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.GuardURL+"/v1/heartbeat", bytes.NewReader(body))
	if err != nil {
		return policy, err
	}
	resp, err := p.guard.Do(req)
	if err != nil {
		return policy, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return policy, fmt.Errorf("guard replied %s", resp.Status)
	}
	// Decoded before the return statement: Go does not say whether a
	// return reads the variable before or after a call that fills it.
	err = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&policy)
	return policy, err
}

// apply puts a policy in force. A throttle can only lower the static rate,
// never raise it, whatever the guard sends.
func (p *Proxy) apply(policy control.Policy) {
	rate := p.cfg.Rate
	if policy.Throttle {
		rate = min(rate, max(0, policy.EventRate))
	}
	p.bucket.setRate(rate)
	p.mu.Lock()
	changed := policy != p.policy
	p.policy = policy
	p.mu.Unlock()
	if changed {
		slog.Info("policy changed", "throttle", policy.Throttle, "event_rate", policy.EventRate, "block_egress", policy.BlockEgress)
	}
}

// Policy returns the policy in force.
func (p *Proxy) Policy() control.Policy {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.policy
}
