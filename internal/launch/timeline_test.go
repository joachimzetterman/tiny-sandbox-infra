package launch

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

func TestTimelineTellsContainedFromActedOnTooEarly(t *testing.T) {
	now := time.Now()
	began := now.Add(-20 * time.Second)
	flood := func(id string, start time.Duration) scenario.Sandbox {
		return scenario.Sandbox{ID: id, Behaviour: "flood", Start: start}
	}
	sc := &scenario.Scenario{Tenants: []scenario.Tenant{{ID: "t1", Tier: scenario.TierFree, Sandboxes: []scenario.Sandbox{
		flood("late", 5*time.Second),   // onset 15 s ago
		flood("early", 30*time.Second), // onset 10 s from now
		flood("free", 5*time.Second),   // never acted on
		{ID: "good", Behaviour: "normal"},
		{ID: "calm", Behaviour: "normal"},
	}}}}
	throttle := func(seq uint64, id string, ago time.Duration) guard.Decision {
		return guard.Decision{Seq: seq, SandboxID: id, Rule: "flood", Effect: guard.Throttle, Rate: 10, At: now.Add(-ago)}
	}
	decisions := []guard.Decision{
		throttle(1, "late", 12*time.Second), // 3 s after its onset
		throttle(2, "early", 5*time.Second), // 15 s before its onset
		throttle(3, "good", 5*time.Second),
		{Seq: 4, SandboxID: "calm", Rule: "candidate", Action: guard.Kill, Note: guard.NoteShadow, At: now},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") != "0" {
			decisions = nil // the timeline has seen them all
		}
		_ = json.NewEncoder(w).Encode(decisions)
	}))
	defer srv.Close()

	l := &Launcher{GuardURL: srv.URL, Clock: clock.Real{}, began: began, started: map[string]time.Time{}}
	for _, s := range sc.Tenants[0].Sandboxes {
		l.started[s.ID] = began
	}
	var out bytes.Buffer
	if err := l.Watch(context.Background(), sc, 700*time.Millisecond, &out); err != nil {
		t.Fatal(err)
	}
	_, summary, _ := strings.Cut(out.String(), "Summary")
	for _, want := range []string{
		"late flood    contained 3.0 s after onset",
		"early flood    no new action after onset",
		"early flood    FALSE POSITIVE",
		"free flood    NOT contained",
		"good normal   FALSE POSITIVE",
		"neighbours: 2 well-behaved sandboxes, 1 throttled, blocked or killed",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary lacks %q", want)
		}
	}
	if strings.Contains(summary, "calm") || strings.Contains(summary, "early flood    NOT contained") {
		t.Error("a shadow decision counted as an action, or an early action as none")
	}
	if t.Failed() {
		t.Log(out.String())
	}
}
