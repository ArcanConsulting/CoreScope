package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForStatus polls AsyncMigrationStatus until it matches `want` or `deadline` passes.
func waitForStatus(t *testing.T, s *Store, name, want string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var status string
	var err error
	for time.Now().Before(deadline) {
		status, err = s.AsyncMigrationStatus(name)
		if err == nil && status == want {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status never reached %q within %s: got %q (err=%v)", want, timeout, status, err)
	return status
}

// TestRunAsyncMigration_PendingThenDone pins the contract for RunAsyncMigration:
//
//   1. After calling, the migration name MUST be queryable in the migrations
//      table with status `pending_async` IMMEDIATELY (no waiting for fn).
//   2. After fn returns, the status MUST transition to `done`.
//   3. RunAsyncMigration MUST return without blocking on fn.
//
// This is the regression test for the recurring "sync migration on large
// table blocks ingestor startup" class (#791, #1483, ...). If this test
// fails the contract is broken — do not relax it; fix the runner.
func TestRunAsyncMigration_PendingThenDone(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})

	const name = "test_async_migration_v1"
	// Wrap the call in a goroutine + select so that a SYNCHRONOUS
	// implementation (one that blocks on fn before returning) would
	// deadlock or hang — proving non-blocking behaviour rather than
	// just "fn started concurrently". A sync impl would never deliver
	// to resultCh because fn blocks on `<-release` which we haven't
	// closed yet.
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- s.RunAsyncMigration(ctx, name, func(ctx context.Context, db *sql.DB) error {
			close(started)
			<-release
			return nil
		})
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("RunAsyncMigration returned error: %v", err)
		}
		// Returned successfully without blocking — that's the contract.
	case <-time.After(2 * time.Second):
		t.Fatal("RunAsyncMigration did not return within 2s — implementation appears to block on fn (sync regression)")
	}

	// Wait for the goroutine to actually start before checking status; this
	// proves RunAsyncMigration did not block on fn and that fn is running
	// concurrently.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("async migration fn did not start within 2s — RunAsyncMigration may never have scheduled")
	}

	status, err := s.AsyncMigrationStatus(name)
	if err != nil {
		t.Fatalf("AsyncMigrationStatus while running: %v", err)
	}
	if status != "pending_async" {
		t.Fatalf("status while fn running: got %q, want %q", status, "pending_async")
	}

	close(release)

	// Poll for transition to done.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, err = s.AsyncMigrationStatus(name)
		if err == nil && status == "done" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status never transitioned to done within 2s: got %q (err=%v)", status, err)
}

// TestRunAsyncMigration_PanicCapture proves that a panic inside fn does NOT
// leak past the recover, AND that the migration row transitions to
// "failed" with the panic message captured — NOT silently to "done".
// Operator visibility into mid-migration crashes is the whole point.
func TestRunAsyncMigration_PanicCapture(t *testing.T) {
	s := newTestStore(t)
	const name = "test_panic_capture_v1"

	if err := s.RunAsyncMigration(context.Background(), name,
		func(ctx context.Context, db *sql.DB) error {
			panic("synthetic boom")
		}); err != nil {
		t.Fatalf("RunAsyncMigration returned error: %v", err)
	}

	s.WaitForAsyncMigrations()

	status, err := s.AsyncMigrationStatus(name)
	if err != nil {
		t.Fatalf("status lookup: %v", err)
	}
	if status != "failed" {
		t.Fatalf("status after panic: got %q, want %q (silent-done would be catastrophic)", status, "failed")
	}

	var errMsg sql.NullString
	if err := s.db.QueryRow(`SELECT error FROM _async_migrations WHERE name = ?`, name).Scan(&errMsg); err != nil {
		t.Fatalf("error column lookup: %v", err)
	}
	if !errMsg.Valid || errMsg.String == "" {
		t.Fatalf("error column empty after panic — operator has no clue what failed")
	}
}

// TestRunAsyncMigration_IdempotentSecondCallNoOps verifies that calling
// RunAsyncMigration a second time with the same name AFTER it has reached
// "done" status does NOT re-run fn. This protects the prod path: ingestor
// restarts must not rebuild already-built indexes.
func TestRunAsyncMigration_IdempotentSecondCallNoOps(t *testing.T) {
	s := newTestStore(t)
	const name = "test_idempotent_v1"

	var calls int32
	fn := func(ctx context.Context, db *sql.DB) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	if err := s.RunAsyncMigration(context.Background(), name, fn); err != nil {
		t.Fatalf("first call: %v", err)
	}
	s.WaitForAsyncMigrations()
	waitForStatus(t, s, name, "done", 2*time.Second)

	// Second call must short-circuit; fn must not be invoked again.
	if err := s.RunAsyncMigration(context.Background(), name, fn); err != nil {
		t.Fatalf("second call: %v", err)
	}
	s.WaitForAsyncMigrations()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn invoked %d times, want 1 (done-state row must short-circuit)", got)
	}
}

// TestRunAsyncMigration_RestartSafetyFailedIsRetried simulates a crashed
// previous run: a row exists in `failed` state from a prior boot. The next
// RunAsyncMigration call MUST re-schedule fn (reset to pending_async, then
// run it), not leave the migration stuck in `failed` forever.
func TestRunAsyncMigration_RestartSafetyFailedIsRetried(t *testing.T) {
	s := newTestStore(t)
	const name = "test_restart_failed_v1"

	if err := ensureAsyncMigrationsTable(s.db); err != nil {
		t.Fatalf("ensure table: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO _async_migrations (name, status, error) VALUES (?, 'failed', 'simulated prior crash')`, name); err != nil {
		t.Fatalf("seed failed row: %v", err)
	}

	var calls int32
	if err := s.RunAsyncMigration(context.Background(), name,
		func(ctx context.Context, db *sql.DB) error {
			atomic.AddInt32(&calls, 1)
			return nil
		}); err != nil {
		t.Fatalf("RunAsyncMigration on failed row: %v", err)
	}
	s.WaitForAsyncMigrations()
	waitForStatus(t, s, name, "done", 2*time.Second)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn invoked %d times, want 1 (failed-state row must be retried)", got)
	}

	// And the error column must be cleared on success.
	var errCol sql.NullString
	if err := s.db.QueryRow(`SELECT error FROM _async_migrations WHERE name = ?`, name).Scan(&errCol); err != nil {
		t.Fatalf("error col: %v", err)
	}
	if errCol.Valid && errCol.String != "" {
		t.Fatalf("error column not cleared on retry success: %q", errCol.String)
	}
}

// TestRunAsyncMigration_RestartSafetyPendingIsRetried simulates the
// ingestor crashing while a migration was still in `pending_async` (the
// goroutine never finished). On next boot the migration MUST be re-picked-up
// — leaving it stuck in pending forever would be a silent prod outage.
func TestRunAsyncMigration_RestartSafetyPendingIsRetried(t *testing.T) {
	s := newTestStore(t)
	const name = "test_restart_pending_v1"

	if err := ensureAsyncMigrationsTable(s.db); err != nil {
		t.Fatalf("ensure table: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO _async_migrations (name, status) VALUES (?, 'pending_async')`, name); err != nil {
		t.Fatalf("seed pending row: %v", err)
	}

	var calls int32
	if err := s.RunAsyncMigration(context.Background(), name,
		func(ctx context.Context, db *sql.DB) error {
			atomic.AddInt32(&calls, 1)
			return nil
		}); err != nil {
		t.Fatalf("RunAsyncMigration on pending row: %v", err)
	}
	s.WaitForAsyncMigrations()
	waitForStatus(t, s, name, "done", 2*time.Second)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn invoked %d times, want 1 (pending row must be retried after crash)", got)
	}
}

// TestRunAsyncMigration_FnErrorRecorded covers the non-panic failure path:
// fn returns an error → status MUST be "failed" with the error captured.
func TestRunAsyncMigration_FnErrorRecorded(t *testing.T) {
	s := newTestStore(t)
	const name = "test_fn_error_v1"

	if err := s.RunAsyncMigration(context.Background(), name,
		func(ctx context.Context, db *sql.DB) error {
			return fmt.Errorf("simulated migration error")
		}); err != nil {
		t.Fatalf("RunAsyncMigration: %v", err)
	}
	s.WaitForAsyncMigrations()

	status, err := s.AsyncMigrationStatus(name)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("status: got %q, want failed", status)
	}

	var errCol sql.NullString
	if err := s.db.QueryRow(`SELECT error FROM _async_migrations WHERE name = ?`, name).Scan(&errCol); err != nil {
		t.Fatalf("error col: %v", err)
	}
	if !errCol.Valid || errCol.String == "" {
		t.Fatalf("error column empty after fn error")
	}
}

// TestRunAsyncMigration_SameNameConcurrent_FnRunsOnce validates the
// single-process concurrency invariant: many goroutines calling
// RunAsyncMigration(name=X) at the same instant must NEVER execute fn more
// than once, AND every caller must receive a nil error (none should hit
// the "UNIQUE constraint failed" race that the previous SELECT-then-INSERT
// implementation was vulnerable to).
//
// The fix relies on:
//   - atomic INSERT ... ON CONFLICT DO UPDATE RETURNING (no SELECT-then-INSERT TOCTOU)
//   - sync.Map in-process guard (no double-launch on shared pending_async)
//
// Previously named TestRunAsyncMigration_ConcurrentSameNameSerialized; the
// old test was tautological (it asserted calls ∈ [1..5], satisfiable by
// any broken impl).
func TestRunAsyncMigration_SameNameConcurrent_FnRunsOnce(t *testing.T) {
	s := newTestStore(t)
	const name = "test_concurrent_serialize_v1"
	const callers = 5

	var calls int32
	fn := func(ctx context.Context, db *sql.DB) error {
		atomic.AddInt32(&calls, 1)
		// Hold long enough to guarantee all other goroutines have
		// observed the in-flight state and exited their RunAsyncMigration
		// returns. Without this the in-process guard cleanup could race
		// with late entrants.
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	// Start-barrier so all goroutines wake at the same instant — maximizes
	// the chance of triggering any latent race.
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- s.RunAsyncMigration(context.Background(), name, fn)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	// Capture errors — every caller MUST get nil. Old impl was prone to
	// "UNIQUE constraint failed: _async_migrations.name" on the loser of
	// the SELECT-then-INSERT race.
	for err := range errs {
		if err != nil {
			t.Fatalf("RunAsyncMigration returned error from concurrent caller: %v", err)
		}
	}

	s.WaitForAsyncMigrations()
	waitForStatus(t, s, name, "done", 2*time.Second)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn invoked %d times, want exactly 1 (concurrent same-name calls must serialize to one execution)", got)
	}
}

// TestRunAsyncMigration_NilDBReturnsError ensures that calling
// RunAsyncMigration on a *Store with a nil db handle returns a clear
// sentinel error rather than panicking. This is a programmer-error guard:
// the helper is concurrency-critical and a nil-deref panic would crash
// the ingestor in a confusing way.
func TestRunAsyncMigration_NilDBReturnsError(t *testing.T) {
	var s Store // zero value, db is nil
	err := s.RunAsyncMigration(context.Background(), "nil_db_test",
		func(ctx context.Context, db *sql.DB) error { return nil })
	if err == nil {
		t.Fatal("RunAsyncMigration on nil db: got nil error, want ErrNilStoreDB")
	}
	if err != ErrNilStoreDB {
		t.Fatalf("RunAsyncMigration on nil db: got %v, want ErrNilStoreDB", err)
	}

	// Same for AsyncMigrationStatus — must not panic.
	if _, err := s.AsyncMigrationStatus("nil_db_test"); err != ErrNilStoreDB {
		t.Fatalf("AsyncMigrationStatus on nil db: got %v, want ErrNilStoreDB", err)
	}
}

// TestRunAsyncMigration_CtxCancelRecorded verifies that when ctx is
// cancelled and fn returns ctx.Err(), the migration is recorded as
// `failed` with "context canceled" in the error column. This is the
// graceful-shutdown contract: a cancelled migration should NOT be marked
// done (it didn't finish), but the failure mode must be observable.
func TestRunAsyncMigration_CtxCancelRecorded(t *testing.T) {
	s := newTestStore(t)
	const name = "test_ctx_cancel_v1"

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	if err := s.RunAsyncMigration(ctx, name, func(ctx context.Context, db *sql.DB) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatalf("RunAsyncMigration: %v", err)
	}

	<-started
	cancel()
	s.WaitForAsyncMigrations()

	status, err := s.AsyncMigrationStatus(name)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("status after ctx cancel: got %q, want failed", status)
	}

	var errCol sql.NullString
	if err := s.db.QueryRow(`SELECT error FROM _async_migrations WHERE name = ?`, name).Scan(&errCol); err != nil {
		t.Fatalf("error col: %v", err)
	}
	if !errCol.Valid || !strings.Contains(errCol.String, "context canceled") {
		t.Fatalf("error column should contain %q, got %q", "context canceled", errCol.String)
	}
}

// TestRunAsyncMigration_CrossProcessSerialized verifies the cross-process
// concurrency claim made in MIGRATIONS.md: two distinct *sql.DB handles
// (simulating two ingestor instances) opened against the same file,
// concurrently calling RunAsyncMigration(name=X), must NEVER execute fn
// more than once total. SQLite serializes writes via its file-level lock,
// and the atomic INSERT ON CONFLICT pattern means the loser sees the
// winner's `pending_async` row and short-circuits.
//
// NOTE: the in-process sync.Map guard does NOT cross processes; the
// cross-process safety comes solely from SQLite's write serialization +
// the atomic ON CONFLICT. This test pins that property.
func TestRunAsyncMigration_CrossProcessSerialized(t *testing.T) {
	// Two stores against the same DB file simulate two processes.
	dir := t.TempDir()
	dbPath := dir + "/cross.db"

	s1, err := OpenStore(dbPath)
	if err != nil {
		t.Fatalf("open s1: %v", err)
	}
	defer s1.Close()

	s2, err := OpenStore(dbPath)
	if err != nil {
		t.Fatalf("open s2: %v", err)
	}
	defer s2.Close()

	// Sanity: distinct *sql.DB handles.
	if s1.db == s2.db {
		t.Fatal("expected distinct *sql.DB handles")
	}

	const name = "test_cross_process_v1"
	var calls int32
	fn := func(ctx context.Context, db *sql.DB) error {
		atomic.AddInt32(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		if err := s1.RunAsyncMigration(context.Background(), name, fn); err != nil {
			t.Errorf("s1.RunAsyncMigration: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		if err := s2.RunAsyncMigration(context.Background(), name, fn); err != nil {
			t.Errorf("s2.RunAsyncMigration: %v", err)
		}
	}()
	close(start)
	wg.Wait()

	s1.WaitForAsyncMigrations()
	s2.WaitForAsyncMigrations()

	// Both stores should agree status==done; SQLite write serialization
	// guarantees the second INSERT sees the first's row.
	st1, _ := s1.AsyncMigrationStatus(name)
	st2, _ := s2.AsyncMigrationStatus(name)
	if st1 != "done" || st2 != "done" {
		t.Fatalf("expected done/done, got s1=%q s2=%q", st1, st2)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn invoked %d times across two processes, want exactly 1 (SQLite write serialization + ON CONFLICT must dedupe)", got)
	}
}
