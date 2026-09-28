package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/web"
)

func mustPermit(t *testing.T, _ *Runtime) web.TensionPermit {
	t.Helper()
	now := time.Now()
	return web.TensionPermit{Allowed: true, IssuedAt: now, ExpiresAt: now.Add(time.Second)}
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{
		DataDir:           t.TempDir(),
		FestoonCapacity:   120,
		RegisterGap:       500 * time.Millisecond,
		RegisterTolerance: 2,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	return runtime
}

func feedCurrentJob(t *testing.T, runtime *Runtime, ctx context.Context) {
	t.Helper()
	if err := runtime.ExecuteOperation(ctx, "load_roll"); err != nil {
		t.Fatalf("load_roll: %v", err)
	}
	if err := runtime.ExecuteOperation(ctx, "feed_web"); err != nil {
		t.Fatalf("feed_web: %v", err)
	}
}

// Cancelling the current job must stop the unwind feed and revoke the web
// threading qualification in the same call.
func TestCancelCurrentStopsFeedAndUnthreadsWeb(t *testing.T) {
	ctx := context.Background()
	runtime := newTestRuntime(t)
	feedCurrentJob(t, runtime, ctx)

	current, ok := runtime.CurrentJob()
	if !ok {
		t.Fatal("expected a current job")
	}
	if _, _, owner, speed := runtime.unwind.Snapshot(); owner != string(current.ID) || speed == 0 {
		t.Fatalf("precondition: feed should run, got owner=%q speed=%v", owner, speed)
	}
	if _, _, _, threaded, _ := runtime.web.Snapshot(); !threaded {
		t.Fatal("precondition: web path should be threaded")
	}

	cancellation, err := runtime.CancelCurrent(ctx)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if job, _ := runtime.CurrentJob(); job.State != "cancelled" {
		t.Fatalf("job state = %q, want cancelled", job.State)
	}
	if _, _, owner, speed := runtime.unwind.Snapshot(); owner != "" || speed != 0 {
		t.Fatalf("feed not stopped after cancel: owner=%q speed=%v", owner, speed)
	}
	if _, _, _, threaded, _ := runtime.web.Snapshot(); threaded {
		t.Fatal("web path still shows threaded after cancel")
	}
	// Further web advance must be rejected.
	if err := runtime.webControl.Advance(string(current.ID), 1, mustPermit(t, runtime), time.Now()); err == nil {
		t.Fatal("web advance must be rejected after cancel")
	}

	gotUnwind, gotWeb := false, false
	for _, component := range cancellation.Components {
		switch component {
		case "unwind":
			gotUnwind = true
		case "web":
			gotWeb = true
		}
	}
	if !gotUnwind || !gotWeb {
		t.Fatalf("cancellation components = %v, want unwind and web", cancellation.Components)
	}
}

// A repeat cancel must be rejected and perform no side effects.
func TestCancelCurrentIsIdempotent(t *testing.T) {
	ctx := context.Background()
	runtime := newTestRuntime(t)
	feedCurrentJob(t, runtime, ctx)

	if _, err := runtime.CancelCurrent(ctx); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	if _, err := runtime.CancelCurrent(ctx); err == nil {
		t.Fatal("second cancel of the same job must be rejected")
	}
}

// A cancel signal belonging to the old job must not stop the new job's roll.
func TestOldJobCancelSignalCannotStopNewJobFeed(t *testing.T) {
	ctx := context.Background()
	runtime := newTestRuntime(t)
	feedCurrentJob(t, runtime, ctx)

	oldJob, _ := runtime.CurrentJob()
	if _, err := runtime.CancelCurrent(ctx); err != nil {
		t.Fatalf("cancel old job: %v", err)
	}

	// A new job takes over, loads a roll and starts feeding.
	newJob := runtime.jobs.Create(time.Now())
	newJob.RollGeneration = 1
	newJob.PlateGeneration = 1
	if err := runtime.jobs.Put(newJob); err != nil {
		t.Fatalf("put new job: %v", err)
	}
	feedCurrentJob(t, runtime, ctx)
	running, ok := runtime.CurrentJob()
	if !ok || running.ID != newJob.ID {
		t.Fatalf("current job = %v, want new job %s", running.ID, newJob.ID)
	}
	_, newRollGeneration, newOwner, newSpeed := runtime.unwind.Snapshot()
	if newOwner != string(newJob.ID) || newSpeed == 0 {
		t.Fatalf("new job feed precondition failed: owner=%q speed=%v", newOwner, newSpeed)
	}

	// The old job's cancel signal must be rejected as stale...
	if err := runtime.jobControl.Cancel(oldJob, time.Now()); err == nil {
		t.Fatal("old job cancel must be rejected as stale")
	}
	// ...and must not stop the new job's unwind feed.
	if _, _, owner, speed := runtime.unwind.Snapshot(); owner != newOwner || speed != newSpeed {
		t.Fatalf("new job feed was disturbed by old cancel: owner=%q speed=%v", owner, speed)
	}
	// Direct fenced teardown from the old job identity is a no-op as well.
	if runtime.unwind.StopFeed(string(oldJob.ID), newRollGeneration) {
		t.Fatal("old owner feed stop must be a no-op")
	}
	if runtime.web.Cancel(string(oldJob.ID), oldJob.Generation) {
		t.Fatal("old owner web cancel must be a no-op")
	}
	if _, _, _, threaded, _ := runtime.web.Snapshot(); !threaded {
		t.Fatal("new job web path must stay threaded against old cancel signals")
	}
}
