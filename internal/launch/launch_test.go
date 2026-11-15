package launch

import (
	"net/netip"
	"testing"
)

func TestPairForCarvesNonOverlappingSubnets(t *testing.T) {
	base := netip.MustParsePrefix("10.77.0.0/16")
	first := PairFor(base, 0, "a")
	if first.Subnet.String() != "10.77.0.0/29" || first.ProxyIP.String() != "10.77.0.1" || first.SandboxIP.String() != "10.77.0.2" {
		t.Errorf("first pair = %+v", first)
	}
	// Number 32 crosses into the next /24.
	if got := PairFor(base, 32, "b"); got.Subnet.String() != "10.77.1.0/29" || got.Network != "tsi-sbx-b" {
		t.Errorf("pair 32 = %+v", got)
	}
	var seen []netip.Prefix
	for i := range 20 {
		p := PairFor(base, i, "x")
		if !base.Contains(p.ProxyIP) || !p.Subnet.Contains(p.ProxyIP) || !p.Subnet.Contains(p.SandboxIP) {
			t.Fatalf("pair %d has addresses outside its subnet: %+v", i, p)
		}
		for _, other := range seen {
			if other.Overlaps(p.Subnet) {
				t.Fatalf("pair %d overlaps %s", i, other)
			}
		}
		seen = append(seen, p.Subnet)
	}
}
