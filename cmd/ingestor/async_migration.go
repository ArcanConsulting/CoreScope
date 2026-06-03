// Async migration helper — runs schema/backfill work that may take minutes on
// large prod tables WITHOUT blocking ingestor startup.
//
// MIGRATION ANNOTATION CONVENTION (read this before touching migrations):
//
//   Sync schema/data migrations (CREATE INDEX, ALTER TABLE, UPDATE ... WHERE)
//   that run inline during OpenStore() block the ingestor from accepting
//   packets until they finish. On an empty dev DB they return in milliseconds;
//   at prod scale (1.9M+ observations, 80K+ adverts) they can pin the boot
//   for minutes and trigger restart loops. This regression class has bitten us
//   repeatedly (#791 resolved_path backfill, #1483 obs_observer_ts_idx_v1).
//
//   ANY new CREATE INDEX / ALTER TABLE / data-rewrite migration MUST EITHER:
//     1. Run via Store.RunAsyncMigration(...) below (preferred for backfills
//        and any work that may touch >1K rows). The migration is recorded as
//        `pending_async` immediately, returns to the caller (boot proceeds),
//        and completes in a goroutine. Status flips to `done` (or `failed`
//        with an error message) when fn returns.
//     2. Carry the preflight annotation comment immediately above the
//        migration block, e.g.
//             // PREFLIGHT: async=true reason="<one-line justification>"
//        Use this for migrations that are genuinely cheap at any scale
//        (e.g. ALTER TABLE ADD COLUMN, CREATE INDEX on a known-bounded
//        table). The annotation is grepped by
//        ~/.openclaw/skills/pr-preflight/scripts/check-async-migrations.sh
//        — its absence on a touched migration block is a hard-fail gate.
//
//   See MIGRATIONS.md in the repo root for the full policy and examples.

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// ErrNilStoreDB is returned by RunAsyncMigration when called on a *Store
// whose db handle is nil. This is a programmer error (the Store was never
// opened, or it was closed) but we surface it as a normal error rather than
// panicking so a misconfigured caller can degrade gracefully.
var ErrNilStoreDB = errors.New("async migration: store has nil db")

// inflightAsyncMigrations tracks which migration names currently have a
// goroutine executing fn IN THIS PROCESS. The SQL row's `pending_async`
// status alone is not sufficient — a row may be `pending_async` because
//   (a) the goroutine is actively running right now in this process, or
//   (b) a previous boot crashed mid-fn and left the row stuck.
// Case (a) must NOT re-launch fn (would run twice in parallel and corrupt
// state). Case (b) must re-launch fn (otherwise the migration is stuck
// forever). The in-process guard is the discriminator: if a name is in the
// map, case (a); if not, case (b).
//
// Cross-process serialization (a second ingestor instance against the same
// DB) is handled by SQLite's write lock + the atomic INSERT ON CONFLICT
// pattern in RunAsyncMigration — see TestRunAsyncMigration_CrossProcess.
var inflightAsyncMigrations sync.Map // map[string]struct{}

// ensureAsyncMigrationsTable creates the bookkeeping table used by
// RunAsyncMigration / AsyncMigrationStatus. Idempotent.
func ensureAsyncMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS _async_migrations (
			name       TEXT PRIMARY KEY,
			status     TEXT NOT NULL,             -- pending_async | done | failed
			started_at TEXT NOT NULL DEFAULT (datetime('now')),
			ended_at   TEXT,
			error      TEXT
		)
	`)
	return err
}

// RunAsyncMigration registers `name` as a pending async migration and
// schedules `fn` to run in a background goroutine. It returns to the caller
// immediately so the ingestor can keep booting.
//
// Caller's obligations on `fn`:
//
//   - `fn` MUST be idempotent. Across crashes, restarts, and retries (and
//     in older buggy versions of this helper, possibly within a single
//     process), `fn` can be invoked more than once for the same name.
//     Always use `IF NOT EXISTS` / `INSERT ... ON CONFLICT DO NOTHING` /
//     equivalent constructs. Never rely on "this only ever runs once".
//   - `fn` SHOULD respect ctx cancellation. The ctx passed in is the
//     ctx passed to RunAsyncMigration; if the caller cancels it (e.g.
//     graceful shutdown), `fn` is expected to return `ctx.Err()` promptly.
//     When `fn` returns a context cancellation error the row is marked
//     `failed` so a future boot will retry.
//   - `fn` will hold the single SQLite write connection for its entire
//     duration. The ingestor opens SQLite with `SetMaxOpenConns(1)`,
//     which means any `fn` that issues a write blocks ALL live ingest
//     writes until it yields. For multi-minute work, use chunked /
//     batched patterns with `time.Sleep` between batches (see
//     `BackfillPathJSONAsync` for the canonical pattern).
//
// Contract (pinned by async_migration_test.go):
//   - status is `pending_async` IMMEDIATELY after this returns.
//   - fn runs in a goroutine; on success status becomes `done`, on error
//     or panic status becomes `failed` and the error is recorded.
//   - Idempotent at the call site: if a row with the same name already
//     exists in `done` state, fn is NOT re-run. If a goroutine for this
//     name is already in flight IN THIS PROCESS, fn is NOT re-run
//     (the in-process guard short-circuits). If in `failed` or
//     `pending_async` (from a crashed prior boot) state, fn IS
//     re-scheduled.
//   - Concurrency-safe: concurrent calls with the same name execute fn
//     AT MOST ONCE. See TestRunAsyncMigration_SameNameConcurrent_FnRunsOnce.
//   - The caller's WaitGroup tracks the goroutine so tests/shutdown can
//     wait via Store.WaitForAsyncMigrations().
func (s *Store) RunAsyncMigration(ctx context.Context, name string, fn func(context.Context, *sql.DB) error) error {
	if s == nil || s.db == nil {
		return ErrNilStoreDB
	}
	if err := ensureAsyncMigrationsTable(s.db); err != nil {
		return fmt.Errorf("ensure _async_migrations: %w", err)
	}

	// Atomic INSERT-or-fetch via SQLite's ON CONFLICT ... DO UPDATE ...
	// RETURNING. SQLite returns the post-statement row state, so:
	//   - On INSERT: returns 'pending_async' (the just-inserted value).
	//   - On CONFLICT: the DO UPDATE is a no-op (SET status=status),
	//     and RETURNING gives the EXISTING status.
	// This collapses the previous SELECT-then-INSERT/UPDATE into a
	// single statement, eliminating the TOCTOU race where two
	// concurrent callers both saw ErrNoRows and one hit a UNIQUE
	// constraint failure on the second INSERT.
	var status string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO _async_migrations (name, status) VALUES (?, 'pending_async')
		ON CONFLICT(name) DO UPDATE SET status = _async_migrations.status
		RETURNING status`,
		name).Scan(&status)
	if err != nil {
		return fmt.Errorf("register async migration %q: %w", name, err)
	}

	if status == "done" {
		return nil // already complete, nothing to do
	}

	// In-process guard: prevent re-entry when a goroutine for this name
	// is already executing fn in THIS process. Without this guard, two
	// concurrent callers both see `pending_async` (one inserted it, one
	// hit ON CONFLICT) and both interpret it as "previous run crashed,
	// retry" — running fn twice in parallel.
	//
	// LoadOrStore is atomic; the loser of the race takes the early-return
	// path. The winner clears the guard with `defer` in the goroutine,
	// guaranteeing no leak even on panic.
	if _, loaded := inflightAsyncMigrations.LoadOrStore(name, struct{}{}); loaded {
		// Another goroutine in this process is already running fn for
		// this name. Don't launch a duplicate.
		return nil
	}

	// We own the in-flight slot. From here on we MUST launch the
	// goroutine (which clears the slot via defer) so the slot doesn't
	// leak on an early error return. If the reset UPDATE fails we
	// still launch a goroutine that immediately exits with the error
	// recorded; otherwise an error here would orphan the inflight map
	// entry and lock out all future retries.

	// Reset the row to a fresh pending_async (clear ended_at/error from
	// any prior failed run). Safe to do AFTER the in-process guard
	// because we now hold exclusive run rights for this name.
	if _, err := s.db.ExecContext(ctx, `
		UPDATE _async_migrations
		SET status = 'pending_async', started_at = datetime('now'), ended_at = NULL, error = NULL
		WHERE name = ?`, name); err != nil {
		// Release the guard and surface the error — we never launched fn.
		inflightAsyncMigrations.Delete(name)
		return fmt.Errorf("reset async migration %q: %w", name, err)
	}

	s.backfillWg.Add(1)
	go func() {
		defer s.backfillWg.Done()
		defer inflightAsyncMigrations.Delete(name)
		var runErr error
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("panic: %v", r)
				log.Printf("[async-migration] %q panic recovered: %v", name, r)
			}
			if runErr != nil {
				recordTerminalStatus(s.db, name, "failed", runErr.Error())
				log.Printf("[async-migration] %q FAILED: %v", name, runErr)
				return
			}
			recordTerminalStatus(s.db, name, "done", "")
			log.Printf("[async-migration] %q done", name)
		}()
		log.Printf("[async-migration] %q starting (boot continues)", name)
		runErr = fn(ctx, s.db)
	}()

	return nil
}

// recordTerminalStatus writes the final status (`done` | `failed`) for an
// async migration, retrying with exponential-ish backoff on transient
// errors (disk full briefly, db locked beyond busy_timeout). If all retries
// fail it logs a CRITICAL message so the row's stuck state is visible in
// ops dashboards / log aggregation — otherwise a row stuck in
// `pending_async` causes the migration to re-run on every subsequent boot,
// silently.
func recordTerminalStatus(db *sql.DB, name, status, errMsg string) {
	backoffs := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= len(backoffs); attempt++ {
		var err error
		if status == "done" {
			_, err = db.Exec(`
				UPDATE _async_migrations
				SET status = 'done', ended_at = datetime('now'), error = NULL
				WHERE name = ?`, name)
		} else {
			_, err = db.Exec(`
				UPDATE _async_migrations
				SET status = ?, ended_at = datetime('now'), error = ?
				WHERE name = ?`, status, errMsg, name)
		}
		if err == nil {
			if attempt > 0 {
				log.Printf("[async-migration] recorded terminal status %q for %q on retry %d", status, name, attempt)
			}
			return
		}
		lastErr = err
		if attempt < len(backoffs) {
			log.Printf("[async-migration] transient error recording terminal status %q for %q (attempt %d/%d): %v", status, name, attempt+1, len(backoffs)+1, err)
			time.Sleep(backoffs[attempt])
		}
	}
	log.Printf("[async-migration] CRITICAL: cannot record terminal status %q for %q after %d attempts, manual intervention required (last error: %v)", status, name, len(backoffs)+1, lastErr)
}

// AsyncMigrationStatus returns the current status of an async migration
// (one of "pending_async", "done", "failed") or sql.ErrNoRows if no such
// migration has been registered.
func (s *Store) AsyncMigrationStatus(name string) (string, error) {
	if s == nil || s.db == nil {
		return "", ErrNilStoreDB
	}
	if err := ensureAsyncMigrationsTable(s.db); err != nil {
		return "", err
	}
	var status string
	err := s.db.QueryRow(`SELECT status FROM _async_migrations WHERE name = ?`, name).Scan(&status)
	return status, err
}

// WaitForAsyncMigrations blocks until all currently-scheduled async migrations
// finish. Intended for tests + graceful shutdown; production boot path does NOT
// call this (that's the whole point).
func (s *Store) WaitForAsyncMigrations() {
	s.backfillWg.Wait()
}
