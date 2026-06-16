// Tests for async migration progress columns + rate-limited writes (#1724).

package main

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestAsyncMigrationProgress_ColumnsExist(t *testing.T) {
	s := newTestStore(t)
	s.WaitForAsyncMigrations()
	if err := ensureAsyncMigrationProgressColumns(s.db); err != nil {
		t.Fatalf("ensure cols: %v", err)
	}
	rows, err := s.db.Query(`PRAGMA table_info(_async_migrations)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	have := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		_ = rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk)
		have[name] = true
	}
	for _, want := range []string{"rows_processed", "rows_total", "last_update_at"} {
		if !have[want] {
			t.Errorf("missing column %q", want)
		}
	}
}

func TestAsyncMigrationProgress_RateLimited(t *testing.T) {
	s := newTestStore(t)
	s.WaitForAsyncMigrations()
	const name = "test_rate_limit_v1"
	// Register the migration row.
	if err := s.RunAsyncMigration(context.Background(), name, func(ctx context.Context, d *sql.DB) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.WaitForAsyncMigrations()

	// First write: should land.
	if err := recordAsyncMigrationProgress(s.db, name, 10, 100); err != nil {
		t.Fatal(err)
	}
	// Second write immediately: should be suppressed (still equals 10).
	if err := recordAsyncMigrationProgress(s.db, name, 20, 100); err != nil {
		t.Fatal(err)
	}
	var p, total int64
	_ = s.db.QueryRow(`SELECT rows_processed, rows_total FROM _async_migrations WHERE name=?`, name).Scan(&p, &total)
	if p != 10 {
		t.Errorf("rate-limited write leaked through: processed=%d, want 10", p)
	}

	// Terminal write: forces past the limiter.
	if err := recordAsyncMigrationProgressTerminal(s.db, name, 999, 100); err != nil {
		t.Fatal(err)
	}
	_ = s.db.QueryRow(`SELECT rows_processed FROM _async_migrations WHERE name=?`, name).Scan(&p)
	if p != 999 {
		t.Errorf("terminal write not honored: processed=%d, want 999", p)
	}
}

func TestAsyncMigrationProgress_ResetOnRetry(t *testing.T) {
	s := newTestStore(t)
	s.WaitForAsyncMigrations()
	const name = "test_retry_reset_v1"

	// Run once, write some progress, fail it.
	if err := s.RunAsyncMigration(context.Background(), name, func(ctx context.Context, d *sql.DB) error {
		_ = recordAsyncMigrationProgressTerminal(d, name, 42, 100)
		return errSentinel{}
	}); err != nil {
		t.Fatal(err)
	}
	s.WaitForAsyncMigrations()

	// Re-register: rows_processed must reset to 0.
	if err := s.RunAsyncMigration(context.Background(), name, func(ctx context.Context, d *sql.DB) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.WaitForAsyncMigrations()
	var p int64
	_ = s.db.QueryRow(`SELECT rows_processed FROM _async_migrations WHERE name=?`, name).Scan(&p)
	if p != 0 {
		t.Errorf("retry did not reset rows_processed: got %d, want 0", p)
	}
}

type errSentinel struct{}

func (errSentinel) Error() string { return "sentinel" }

func TestAsyncMigrationProgress_TerminalForcesWithinSecond(t *testing.T) {
	s := newTestStore(t)
	s.WaitForAsyncMigrations()
	const name = "test_terminal_force_v1"
	if err := s.RunAsyncMigration(context.Background(), name, func(ctx context.Context, d *sql.DB) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.WaitForAsyncMigrations()
	_ = recordAsyncMigrationProgress(s.db, name, 1, 10)
	time.Sleep(5 * time.Millisecond)
	_ = recordAsyncMigrationProgressTerminal(s.db, name, 10, 10)
	var p int64
	_ = s.db.QueryRow(`SELECT rows_processed FROM _async_migrations WHERE name=?`, name).Scan(&p)
	if p != 10 {
		t.Errorf("terminal within rate window not honored: got %d, want 10", p)
	}
}
