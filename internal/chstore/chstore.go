// Package chstore is the only code that talks to ClickHouse.
package chstore

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

type Store struct {
	conn  driver.Conn
	clock clock.Clock
}

// Open prepares a connection pool. It does not connect yet, so a store
// that is still starting is not an error here: the first insert fails and
// the caller retries.
func Open(addr, database, user, password string, c clock.Clock) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{addr},
		Auth:        clickhouse.Auth{Database: database, Username: user, Password: password},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Store{conn: conn, clock: c}, nil
}

// insert sends n rows as one insert. row returns the column values of row i.
func (s *Store) insert(ctx context.Context, query string, n int, row func(i int) []any) error {
	if n == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	defer batch.Close()
	for i := range n {
		// Go idiom: row(i)... spreads the slice into separate arguments.
		if err := batch.Append(row(i)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *Store) InsertEvents(ctx context.Context, evs []event.Event) error {
	now := s.clock.Now()
	return s.insert(ctx, "INSERT INTO events (tenant_id, sandbox_id, event_time, event_id, type, payload, received_at)", len(evs),
		func(i int) []any {
			e := evs[i]
			return []any{e.TenantID, e.SandboxID, e.EventTime, e.EventID, e.Type, e.Payload, now}
		})
}

// InsertActions writes the guard's audit trail.
func (s *Store) InsertActions(ctx context.Context, rows []guard.Decision) error {
	return s.insert(ctx, "INSERT INTO guard_actions (at, tenant_id, sandbox_id, tier, rule, mode, action, effect, note, rate, inputs, expires)", len(rows),
		func(i int) []any {
			d := rows[i]
			if d.Inputs == nil {
				d.Inputs = map[string]float64{}
			}
			return []any{d.At, d.TenantID, d.SandboxID, d.Tier, d.Rule, d.Mode, d.Action, d.Effect, d.Note, d.Rate, d.Inputs, d.Expires}
		})
}

// InsertUsage writes one row per heartbeat. The values are the proxy's
// running totals, not the growth since the last row, see schema.sql.
func (s *Store) InsertUsage(ctx context.Context, rows []guard.Usage) error {
	return s.insert(ctx, "INSERT INTO usage (tenant_id, sandbox_id, proxy_started, at, events_accepted, bytes_accepted, events_rejected, egress_bytes, watched_bytes, blocked_calls)", len(rows),
		func(i int) []any {
			u, c := rows[i], rows[i].Counters
			return []any{u.TenantID, u.SandboxID, u.Started, u.At, c.EventsAccepted, c.BytesAccepted, control.Total(c.Rejects),
				control.Total(c.EgressByClass), c.EgressByClass[scenario.ClassWatched], control.Total(c.Blocked)}
		})
}
