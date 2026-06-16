// Progress-write rate limiter + schema-progress columns for async migrations
// (issue #1724).
//
// Long-running async backfills (tx_last_seen_backfill_v1, future ones) need
// to surface progress so operators can see motion during cold-load. Writing
// every per-batch progress update to the bookkeeping table would compete
// with the migration's own writer lock and add an UPDATE per chunk. Cap to
// ≤1 write/sec per migration; force a final write on terminal calls.
//
// The schema columns (rows_processed, rows_total, last_update_at) are
// additive on legacy DBs via ADD COLUMN; only the "duplicate column" error
// is swallowed — every other error propagates. The CREATE TABLE body
// includes the new columns for fresh installs.

package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// progressGate throttles per-migration progress writes.
var (
	progressGateMu        sync.Mutex
	progressLastWriteAt   = map[string]time.Time{}
	progressSchemaWarnOnce sync.Once
)

// ensureAsyncMigrationProgressColumns adds rows_processed / rows_total /
// last_update_at to _async_migrations on legacy DBs. Idempotent. Only the
// "duplicate column" SQLite error is swallowed.
func ensureAsyncMigrationProgressColumns(db *sql.DB) error {
	// Make sure the base table exists first (fresh-install path covered).
	if err := ensureAsyncMigrationsTable(db); err != nil {
		return err
	}
	cols := []struct{ name, typ string }{
		{"rows_processed", "INTEGER NOT NULL DEFAULT 0"},
		{"rows_total", "INTEGER NOT NULL DEFAULT 0"},
		{"last_update_at", "TEXT"},
	}
	for _, c := range cols {
		_, err := db.Exec(fmt.Sprintf(
			`ALTER TABLE _async_migrations ADD COLUMN %s %s`, c.name, c.typ))
		if err != nil && !isDuplicateColumnErr(err) {
			return fmt.Errorf("add column %s: %w", c.name, err)
		}
	}
	return nil
}

func isDuplicateColumnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column")
}

// recordAsyncMigrationProgress writes (processed, total) for `name`, but
// no more than once per second per migration name. Pass terminal=true to
// force the write (used for final stable counts).
func recordAsyncMigrationProgress(db *sql.DB, name string, processed, total int64) error {
	return recordAsyncMigrationProgressEx(db, name, processed, total, false)
}

// recordAsyncMigrationProgressTerminal forces a write of the final stable
// counts, bypassing the rate limiter.
func recordAsyncMigrationProgressTerminal(db *sql.DB, name string, processed, total int64) error {
	return recordAsyncMigrationProgressEx(db, name, processed, total, true)
}

func recordAsyncMigrationProgressEx(db *sql.DB, name string, processed, total int64, terminal bool) error {
	now := time.Now()
	progressGateMu.Lock()
	last, ok := progressLastWriteAt[name]
	if !terminal && ok && now.Sub(last) < time.Second {
		progressGateMu.Unlock()
		return nil
	}
	progressLastWriteAt[name] = now
	progressGateMu.Unlock()

	res, err := db.Exec(`
		UPDATE _async_migrations
		SET rows_processed = ?, rows_total = ?, last_update_at = ?
		WHERE name = ?`,
		processed, total, now.UTC().Format(time.RFC3339), name)
	if err != nil {
		// Most likely schema-missing on a legacy DB that didn't run
		// ensureAsyncMigrationProgressColumns. Log once, never per batch.
		progressSchemaWarnOnce.Do(func() {
			log.Printf("[async-migration] progress write failed (likely missing columns; further such errors suppressed): %v", err)
		})
		return err
	}
	_ = res
	return nil
}

// resetAsyncMigrationProgress wipes per-migration progress fields on retry
// so the new run's denominator is honest.
func resetAsyncMigrationProgress(db *sql.DB, name string) error {
	progressGateMu.Lock()
	delete(progressLastWriteAt, name)
	progressGateMu.Unlock()
	_, err := db.Exec(`
		UPDATE _async_migrations
		SET rows_processed = 0, rows_total = 0, last_update_at = NULL
		WHERE name = ?`, name)
	return err
}
