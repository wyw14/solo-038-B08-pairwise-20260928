package service

import (
	"context"
	"testing"
	"time"
)

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{DataDir: t.TempDir(), FestoonCapacity: 100, RegisterGap: time.Millisecond, RegisterTolerance: 2})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return runtime
}

func feedCurrentJob(t *testing.T, runtime *Runtime) {
	t.Helper()
	if _, err := runtime.CreateOperation(context.Background(), "load_roll"); err != nil {
		t.Fatalf("load_roll: %v", err)
	}
	if _, err := runtime.CreateOperation(context.Background(), "feed_web"); err != nil {
		t.Fatalf("feed_web: %v", err)
	}
}

func unwindSpeed(t *testing.T, runtime *Runtime) float64 {
	t.Helper()
	_, _, _, speed := runtime.unwind.Snapshot()
	return speed
}

func webThreaded(t *testing.T, runtime *Runtime) bool {
	t.Helper()
	_, _, _, threaded, _ := runtime.web.Snapshot()
	return threaded
}

func hasComponent(cancellation Cancellation, name string) bool {
	for _, component := range cancellation.Components {
		if component == name {
			return true
		}
	}
	return false
}

func TestCancelCurrentStopsFeedAndUnthreadsWeb(t *testing.T) {
	runtime := newTestRuntime(t)
	feedCurrentJob(t, runtime)
	current, ok := runtime.CurrentJob()
	if !ok {
		t.Fatal("there is no current job")
	}
	if speed := unwindSpeed(t, runtime); speed <= 0 {
		t.Fatalf("expected feed to be running, got speed %v", speed)
	}
	if !webThreaded(t, runtime) {
		t.Fatal("expected web to be threaded before cancel")
	}
	cancellation, err := runtime.CancelCurrent(context.Background())
	if err != nil {
		t.Fatalf("CancelCurrent: %v", err)
	}
	if cancellation.JobID != current.ID {
		t.Fatalf("cancellation job %q does not match current job %q", cancellation.JobID, current.ID)
	}
	if !hasComponent(cancellation, "unwind") || !hasComponent(cancellation, "web") {
		t.Fatalf("cancellation did not cover unwind and web: %v", cancellation.Components)
	}
	if speed := unwindSpeed(t, runtime); speed != 0 {
		t.Fatalf("expected unwind feed stopped, got speed %v", speed)
	}
	if webThreaded(t, runtime) {
		t.Fatal("expected web unthreaded after cancel")
	}
	if err := runtime.web.Advance(string(current.ID), 1); err == nil {
		t.Fatal("web advance still permitted after cancel")
	}
}

func TestCancelCurrentTwiceIsRejected(t *testing.T) {
	runtime := newTestRuntime(t)
	feedCurrentJob(t, runtime)
	if _, err := runtime.CancelCurrent(context.Background()); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	if _, err := runtime.CancelCurrent(context.Background()); err == nil {
		t.Fatal("expected repeated cancel to be rejected")
	}
}

func TestStaleCancelSignalCannotStopNewJob(t *testing.T) {
	runtime := newTestRuntime(t)
	feedCurrentJob(t, runtime)
	oldJob, ok := runtime.CurrentJob()
	if !ok {
		t.Fatal("there is no current job")
	}
	if _, err := runtime.CancelCurrent(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	runtime.jobs.Create(time.Now())
	feedCurrentJob(t, runtime)
	newJob, ok := runtime.CurrentJob()
	if !ok {
		t.Fatal("there is no current job")
	}
	if newJob.ID == oldJob.ID {
		t.Fatal("expected a distinct new job")
	}
	if speed := unwindSpeed(t, runtime); speed <= 0 {
		t.Fatalf("expected new job feed running, got speed %v", speed)
	}
	if components := runtime.releaseFeed(oldJob); len(components) != 0 {
		t.Fatalf("stale cancel signal tore down components: %v", components)
	}
	if speed := unwindSpeed(t, runtime); speed <= 0 {
		t.Fatalf("stale cancel stopped the new job's feed, got speed %v", speed)
	}
	if !webThreaded(t, runtime) {
		t.Fatal("stale cancel unthreaded the new job's web")
	}
	if _, err := runtime.CancelCurrent(context.Background()); err != nil {
		t.Fatalf("cancel new job: %v", err)
	}
	if speed := unwindSpeed(t, runtime); speed != 0 {
		t.Fatalf("expected new job feed stopped after its own cancel, got speed %v", speed)
	}
	if webThreaded(t, runtime) {
		t.Fatal("expected new job web unthreaded after its own cancel")
	}
}
