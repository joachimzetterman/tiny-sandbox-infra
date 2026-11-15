package guard

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// A recording is a text file with one JSON value per line. The first line
// is a Header, every other line a Record. Replaying the records through a
// guard, with the clock set to each record's time, shows what any rule set
// would have decided about the same traffic.

// Header is the scenario the recording was made from. It is the ground
// truth a replay is scored against: who misbehaves, how, and from when.
type Header struct {
	// Enforced means rules were acting while this was recorded, so the
	// traffic already shows what one rule set did to it. To compare rule
	// sets fairly, record with every rule in shadow.
	Enforced bool              `json:"enforced"`
	Tenants  []scenario.Tenant `json:"tenants"`
}

// Record is one heartbeat and when it arrived.
// Go idiom: a struct named without a field name is embedded. Its fields
// become fields of Record, in Go and in the JSON.
type Record struct {
	At time.Time `json:"at"`
	control.Heartbeat
}

// MaxRecording bounds a recording in bytes. A forgotten recorder must not
// fill the disk.
const MaxRecording = 64 << 20

// tape appends one heartbeat to the recording. The caller holds mu, so the
// lines are in the order the guard judged them. One small write per
// heartbeat is fine for a demo, a busy guard would buffer or sample.
func (g *Guard) tape(now time.Time, hb control.Heartbeat) {
	if g.cfg.Record == nil || g.recorded >= MaxRecording {
		return
	}
	line, err := json.Marshal(Record{At: now, Heartbeat: hb})
	if err == nil {
		var n int
		n, err = g.cfg.Record.Write(append(line, '\n'))
		g.recorded += n
	}
	if err != nil || g.recorded >= MaxRecording {
		slog.Error("recording stopped", "err", err, "bytes", g.recorded)
		g.recorded = MaxRecording
	}
}
