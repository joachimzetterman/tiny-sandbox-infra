package guard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestEveryHeartbeatIsRecordedAsItArrived(t *testing.T) {
	h := newHarness(t, header, nil)
	var tape bytes.Buffer
	h.g.cfg.Record = &tape
	h.add("t1", "s1", "s2")
	h.run(3, steady(20))

	var got []Record
	for lines := bufio.NewScanner(&tape); lines.Scan(); {
		var r Record
		if err := json.Unmarshal(lines.Bytes(), &r); err != nil {
			t.Fatalf("line %d is not a record: %v", len(got)+1, err)
		}
		got = append(got, r)
	}
	if len(got) != 6 {
		t.Fatalf("%d records for 6 heartbeats", len(got))
	}
	// The last one: s2, three seconds in, with its running totals.
	last := got[5]
	if last.SandboxID != "s2" || last.TenantID != "t1" || !last.At.Equal(start.Add(3*time.Second)) || last.Counters.EventsAccepted != 60 {
		t.Errorf("last record is %+v", last)
	}
}

// brokenDisk fails every write.
type brokenDisk struct{ writes int }

func (b *brokenDisk) Write([]byte) (int, error) {
	b.writes++
	return 0, errors.New("disk is full")
}

func TestRecordingThatFailsOrGrowsTooLargeStopsAndTheGuardGoesOn(t *testing.T) {
	h := newHarness(t, killLoud, map[string]string{"t1": "free"})
	disk := &brokenDisk{}
	h.g.cfg.Record = disk
	h.add("t1", "s1")
	h.run(5, steady(500))
	if disk.writes != 1 {
		t.Errorf("%d writes to a recording that failed on the first, want it to stop", disk.writes)
	}
	h.expect("s1", "3s stop kill") // the guard still does its job

	// A recording at its size limit takes no more.
	h = newHarness(t, header, nil)
	var tape bytes.Buffer
	h.g.cfg.Record, h.g.recorded = &tape, MaxRecording-1
	h.add("t1", "s1")
	h.run(3, steady(20))
	if lines := bytes.Count(tape.Bytes(), []byte("\n")); lines != 1 {
		t.Errorf("%d records written at the size limit, want the one that crossed it", lines)
	}
}
