package main

import (
	"testing"
	"time"
)

func TestLimitsFallBackOnValuesThatWouldBreakThem(t *testing.T) {
	for _, v := range []string{"", "abc", "0", "-1"} {
		t.Setenv("TSI_TEST_LIMIT", v)
		if got := number("TSI_TEST_LIMIT", 50); got != 50 {
			t.Errorf("number(%q) = %d, want the default 50", v, got)
		}
	}
	for _, v := range []string{"", "soon", "0", "0s", "-1s"} {
		t.Setenv("TSI_TEST_LIMIT", v)
		if got := duration("TSI_TEST_LIMIT", time.Second); got != time.Second {
			t.Errorf("duration(%q) = %s, want the default 1s", v, got)
		}
	}
	t.Setenv("TSI_TEST_LIMIT", "7")
	if got := number("TSI_TEST_LIMIT", 50); got != 7 {
		t.Errorf("number(7) = %d", got)
	}
	t.Setenv("TSI_TEST_LIMIT", "250ms")
	if got := duration("TSI_TEST_LIMIT", time.Second); got != 250*time.Millisecond {
		t.Errorf("duration(250ms) = %s", got)
	}
}
