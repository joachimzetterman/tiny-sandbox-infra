// Package chstore is the only code that talks to ClickHouse.
package chstore

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
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

// InsertEvents writes all events as one insert.
func (s *Store) InsertEvents(ctx context.Context, evs []event.Event) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO events (tenant_id, sandbox_id, event_time, event_id, type, payload, received_at)")
	if err != nil {
		return err
	}
	defer batch.Close()
	now := s.clock.Now()
	for _, e := range evs {
		if err := batch.Append(e.TenantID, e.SandboxID, e.EventTime, e.EventID, e.Type, e.Payload, now); err != nil {
			return err
		}
	}
	return batch.Send()
}
