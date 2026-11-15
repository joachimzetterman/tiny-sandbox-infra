package scenario

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultScenarioMatchesTheBrief(t *testing.T) {
	sc, err := Load("../../scenarios/default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	byBehaviour, paid, sandboxes := map[string]int{}, 0, 0
	for _, tenant := range sc.Tenants {
		if tenant.Tier == "paid" {
			paid++
		}
		for _, s := range tenant.Sandboxes {
			byBehaviour[s.Behaviour]++
			sandboxes++
		}
	}
	if len(sc.Tenants) != 15 || sandboxes != 17 || paid != 2 {
		t.Errorf("got %d tenants, %d sandboxes, %d paid; want 15, 17, 2", len(sc.Tenants), sandboxes, paid)
	}
	want := map[string]int{"normal": 9, "bursty": 1, "flood": 4, "exfil": 1, "baddest": 2}
	for b, n := range want {
		if byBehaviour[b] != n {
			t.Errorf("%d %s sandboxes, want %d", byBehaviour[b], b, n)
		}
	}
	if got := sc.Sites.Classes(); got["allowed.test"] != ClassTrusted || got["paste.test"] != ClassWatched || got["evil.test"] != ClassBad {
		t.Errorf("site classes wrong: %v", got)
	}
}

func TestADomainListedTwiceEndsUpBlocked(t *testing.T) {
	s := Sites{Trusted: []string{"x.test"}, Bad: []string{"x.test"}}
	if got := s.Classes()["x.test"]; got != ClassBad {
		t.Errorf("class = %q, want bad", got)
	}
}

func TestLoadRejectsBrokenScenarios(t *testing.T) {
	const head = "subnet: 10.77.0.0/16\nlimits: {events_per_sec: 100, burst: 200}\n"
	cases := []struct{ name, yaml, wantErr string }{
		{"unknown tier", head + "tenants: [{id: a, tier: gold}]", "tier"},
		{"unknown behaviour", head + "tenants: [{id: a, tier: free, sandboxes: [{id: s, behaviour: nap}]}]", "behaviour"},
		{"duplicate sandbox id", head + "tenants: [{id: a, tier: free, sandboxes: [{id: s, behaviour: normal}, {id: s, behaviour: normal}]}]", "twice"},
		{"misspelled key", head + "tennants: []", "tennants"},
		{"no limits", "subnet: 10.77.0.0/16\n", "limits"},
		{"subnet too small", "subnet: 10.77.0.0/30\nlimits: {events_per_sec: 1, burst: 1}\n", "subnet"},
		{"more sandboxes than the subnet holds", "subnet: 10.77.0.0/29\nlimits: {events_per_sec: 1, burst: 1}\n" +
			"tenants: [{id: a, tier: free, sandboxes: [{id: s1, behaviour: normal}, {id: s2, behaviour: normal}]}]", "room"},
	}
	for _, c := range cases {
		path := filepath.Join(t.TempDir(), "s.yaml")
		if err := os.WriteFile(path, []byte(c.yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: err = %v, want one mentioning %q", c.name, err, c.wantErr)
		}
	}
}
