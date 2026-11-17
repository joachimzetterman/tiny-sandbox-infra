package guard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

func record(t *testing.T, h *harness, disk io.Writer) *Recorder {
	t.Helper()
	r, err := NewRecorder(disk, Header{})
	if err != nil {
		t.Fatal(err)
	}
	h.g.cfg.Record = r
	return r
}

func TestEveryHeartbeatIsRecordedAsItArrived(t *testing.T) {
	h := newHarness(t, header, nil)
	var tape bytes.Buffer
	r := record(t, h, &tape)
	h.add("t1", "s1", "s2")
	h.run(3, steady(20))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	lines := bufio.NewScanner(&tape)
	lines.Scan() // the header
	var got []Record
	for lines.Scan() {
		var r Record
		if err := json.Unmarshal(lines.Bytes(), &r); err != nil {
			t.Fatalf("line %d is not a record: %v", len(got)+2, err)
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

// brokenDisk takes the header and fails every write after it.
type brokenDisk struct{ writes int }

func (b *brokenDisk) Write(p []byte) (int, error) {
	if b.writes++; b.writes == 1 {
		return len(p), nil
	}
	return 0, errors.New("disk is full")
}

// slowDisk takes the header, then holds the next write until it is let go.
type slowDisk struct {
	writes        int
	writing, free chan struct{}
}

func (s *slowDisk) Write(p []byte) (int, error) {
	if s.writes++; s.writes == 2 {
		close(s.writing)
		<-s.free
	}
	return len(p), nil
}

func TestRecordingThatFailsOrGrowsTooLargeStopsAndTheGuardGoesOn(t *testing.T) {
	h := newHarness(t, killLoud, map[string]string{"t1": "free"})
	disk := &brokenDisk{}
	r := record(t, h, disk)
	h.add("t1", "s1")
	h.run(5, steady(500))
	h.expect("s1", "3s stop kill") // the guard still does its job
	if err := r.Close(); err == nil || disk.writes != 2 {
		t.Errorf("%d writes and %v from Close, want 2 and the error that stopped it", disk.writes, err)
	}

	// A disk that falls behind is not waited for: the heartbeat that finds
	// the queue full is not recorded, and the recording has a gap.
	slow := &slowDisk{writing: make(chan struct{}), free: make(chan struct{})}
	r, err := NewRecorder(slow, Header{})
	if err != nil {
		t.Fatal(err)
	}
	r.add(Record{At: start})
	<-slow.writing
	for range recordQueue + 1 {
		r.add(Record{At: start})
	}
	close(slow.free)
	if err := r.Close(); err == nil {
		t.Error("a recording that dropped a heartbeat says it is whole")
	}

	// A recording at its size limit takes no more, and what it has is whole.
	h = newHarness(t, header, nil)
	var tape bytes.Buffer
	r = record(t, h, &tape)
	r.size = MaxRecording - 1
	h.add("t1", "s1")
	h.run(3, steady(20))
	if err := r.Close(); err != nil {
		t.Errorf("a recording at its size limit says it has a gap: %v", err)
	}
	if lines := bytes.Count(tape.Bytes(), []byte("\n")); lines != 2 {
		t.Errorf("%d lines written at the size limit, want the header and the record that crossed it", lines)
	}
}
