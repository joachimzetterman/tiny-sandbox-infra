// Package scenario is the YAML file that describes a demo: tenants, their
// sandboxes and how each one behaves. Launch reads it to start containers.
package scenario

import (
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Scenario struct {
	// Subnet is carved into one /29 per sandbox, so Docker's default
	// address pools (about 30 networks in total) are never used.
	Subnet  string   `yaml:"subnet"`
	Limits  Limits   `yaml:"limits"`
	Sites   Sites    `yaml:"sites"`
	Tenants []Tenant `yaml:"tenants"`
}

// Limits are the static per-sandbox limits every proxy starts with.
type Limits struct {
	EventsPerSec int `yaml:"events_per_sec"`
	Burst        int `yaml:"burst"`
}

// Sites puts domains into classes. A domain in no class is denied.
type Sites struct {
	Trusted []string `yaml:"trusted"` // allowed
	Watched []string `yaml:"watched"` // allowed, bytes counted separately
	Bad     []string `yaml:"bad"`     // blocked, every attempt counted
}

const (
	ClassTrusted  = "trusted"
	ClassWatched  = "watched"
	ClassBad      = "bad"
	ClassUnlisted = "unlisted"
)

// Classes returns a lookup from domain to class. Names are lower case,
// because that is how the proxy looks them up.
func (s Sites) Classes() map[string]string {
	m := map[string]string{}
	for _, d := range s.Trusted {
		m[strings.ToLower(d)] = ClassTrusted
	}
	for _, d := range s.Watched {
		m[strings.ToLower(d)] = ClassWatched
	}
	for _, d := range s.Bad { // last, so a domain listed twice ends up blocked
		m[strings.ToLower(d)] = ClassBad
	}
	return m
}

type Tenant struct {
	ID        string    `yaml:"id"`
	Tier      string    `yaml:"tier"` // free or paid
	Sandboxes []Sandbox `yaml:"sandboxes"`
}

type Sandbox struct {
	ID        string        `yaml:"id"`
	Behaviour string        `yaml:"behaviour"`
	Start     time.Duration `yaml:"start"` // the sandbox is normal until this offset
	Rate      int           `yaml:"rate"`  // events per second once the behaviour starts, 0 for the default
}

var Behaviours = map[string]bool{
	"normal": true, "bursty": true, "flood": true,
	"exfil": true, "baddest": true, "rejectstorm": true,
}

// Load reads and validates a scenario. It rejects unknown keys, so a typo
// in the file is an error and not a silently ignored setting.
func Load(path string) (*Scenario, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	var sc Scenario
	if err := dec.Decode(&sc); err != nil {
		// Go idiom: %w wraps the original error, so callers can still inspect it.
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &sc, sc.validate()
}

func (sc *Scenario) validate() error {
	prefix, err := netip.ParsePrefix(sc.Subnet)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 29 {
		return fmt.Errorf("subnet %q must be an IPv4 prefix of /29 or larger", sc.Subnet)
	}
	if sc.Limits.EventsPerSec <= 0 || sc.Limits.Burst <= 0 {
		return fmt.Errorf("limits must be positive")
	}
	seen := map[string]bool{}
	for _, t := range sc.Tenants {
		if t.Tier != "free" && t.Tier != "paid" {
			return fmt.Errorf("tenant %s: tier must be free or paid, got %q", t.ID, t.Tier)
		}
		for _, s := range t.Sandboxes {
			if !Behaviours[s.Behaviour] {
				return fmt.Errorf("sandbox %s: unknown behaviour %q", s.ID, s.Behaviour)
			}
			if s.ID == "" || seen[s.ID] {
				return fmt.Errorf("sandbox id %q is empty or used twice", s.ID)
			}
			seen[s.ID] = true
		}
	}
	if room := 1 << (29 - prefix.Bits()); len(seen) > room {
		return fmt.Errorf("subnet %s has room for %d sandboxes, scenario has %d", sc.Subnet, room, len(seen))
	}
	return nil
}
