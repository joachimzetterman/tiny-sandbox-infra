package guard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
)

// fakeSink records what the guard writes and can be told to fail.
type fakeSink struct {
	fail    bool
	actions []Decision
	usage   []Usage
}

func (f *fakeSink) InsertActions(_ context.Context, rows []Decision) error {
	if f.fail {
		return errors.New("store is down")
	}
	f.actions = append(f.actions, rows...)
	return nil
}

func (f *fakeSink) InsertUsage(_ context.Context, rows []Usage) error {
	if f.fail {
		return errors.New("store is down")
	}
	f.usage = append(f.usage, rows...)
	return nil
}

const killLoud = header + `
  - {name: stop, expr: "offered_rate > 100.0", action: kill, mode: enforce, ttl: 1m}`

func TestGuardOverHTTP(t *testing.T) {
	h := newHarness(t, killLoud, map[string]string{"t1": "free"})
	h.add("t1", "s1")
	h.run(5, steady(500)) // s1 is killed
	srv := httptest.NewServer(h.g.Handler())
	defer srv.Close()

	get := func(path string, into any) {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	// A heartbeat is answered with the policy.
	resp, err := http.Post(srv.URL+"/v1/heartbeat", "application/json", strings.NewReader(`{"tenant_id":"t1","sandbox_id":"s1"}`))
	if err != nil {
		t.Fatal(err)
	}
	var policy control.Policy
	_ = json.NewDecoder(resp.Body).Decode(&policy)
	resp.Body.Close()
	if policy != control.CutOff {
		t.Errorf("a killed sandbox's proxy was told %+v", policy)
	}
	if resp, _ := http.Post(srv.URL+"/v1/heartbeat", "application/json", strings.NewReader(`{"no":"sandbox"}`)); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a heartbeat without a sandbox got %d", resp.StatusCode)
	}

	var deny control.DenyList
	get("/v1/deny", &deny)
	if len(deny.Sandboxes) != 1 || deny.Sandboxes[0] != "s1" {
		t.Errorf("deny list is %v", deny.Sandboxes)
	}
	var all, none []Decision
	get("/v1/decisions", &all)
	get("/v1/decisions?after=1", &none)
	if len(all) != 1 || all[0].Effect != Kill || all[0].Seq != 1 || len(none) != 0 {
		t.Errorf("decisions: %+v, after the first: %+v", all, none)
	}
}

func TestStoreThatIsDownDelaysTheRecordAndNotTheGuard(t *testing.T) {
	h := newHarness(t, killLoud, map[string]string{"t1": "free"})
	sink := &fakeSink{fail: true}
	h.g.cfg.Sink = sink
	h.add("t1", "s1")
	h.run(3, steady(500))
	h.g.Flush(context.Background())
	// The guard decided and acted although nothing could be written.
	if len(h.runtime.Killed) != 1 || len(sink.actions) != 0 {
		t.Fatalf("killed %v, rows written %d", h.runtime.Killed, len(sink.actions))
	}

	sink.fail = false
	h.g.Flush(context.Background())
	if len(sink.actions) != 1 || len(sink.usage) != 3 {
		t.Fatalf("after the store came back: %d decisions and %d usage rows, want 1 and 3", len(sink.actions), len(sink.usage))
	}
	// Usage rows carry the running totals, see schema.sql.
	if last := sink.usage[2]; last.TenantID != "t1" || last.SandboxID != "s1" || last.Counters.EventsAccepted != 1500 {
		t.Errorf("usage row %+v", last)
	}
	h.g.Flush(context.Background())
	if len(sink.actions) != 1 || len(sink.usage) != 3 {
		t.Errorf("rows were written twice: %d decisions, %d usage rows", len(sink.actions), len(sink.usage))
	}
}
