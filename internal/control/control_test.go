package control

import "testing"

func TestMergeKeepsTheStricterSideOfEachControl(t *testing.T) {
	slow := Policy{Throttle: true, EventRate: 20}
	slower := Policy{Throttle: true, EventRate: 5}
	blocked := Policy{BlockEgress: true}
	for _, c := range []struct {
		name       string
		a, b, want Policy
	}{
		{"nothing and nothing", Policy{}, Policy{}, Policy{}},
		{"a throttle and nothing", slow, Policy{}, slow},
		{"the lower rate wins", slow, slower, slower},
		{"whichever comes first", slower, slow, slower},
		{"rate zero beats any rate", slow, Policy{Throttle: true}, Policy{Throttle: true}},
		{"events and egress do not mix", slow, blocked, Policy{Throttle: true, EventRate: 20, BlockEgress: true}},
		{"a block stays a block", blocked, slower, Policy{Throttle: true, EventRate: 5, BlockEgress: true}},
	} {
		if got := c.a.Merge(c.b); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
	// Only a throttle to zero stops every event. A policy that does not
	// throttle at all has rate zero too, and that means "no limit from me".
	if (Policy{}).NoEvents() || slow.NoEvents() || blocked.NoEvents() || !CutOff.NoEvents() {
		t.Error("NoEvents is wrong")
	}
}
