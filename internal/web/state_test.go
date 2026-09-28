package web

import "testing"

func TestUnthreadRequiresMatchingFence(t *testing.T) {
	state := NewState()
	if err := state.Fence("job-a", 1, 1); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if err := state.Thread("job-a"); err != nil {
		t.Fatalf("Thread: %v", err)
	}
	if state.Unthread("", 1) {
		t.Fatal("unthread accepted an empty owner")
	}
	if state.Unthread("job-b", 1) {
		t.Fatal("unthread accepted a mismatched owner")
	}
	if state.Unthread("job-a", 2) {
		t.Fatal("unthread accepted a mismatched generation")
	}
	if _, _, _, threaded, _ := state.Snapshot(); !threaded {
		t.Fatal("web unthreaded by a stale signal")
	}
	if !state.Unthread("job-a", 1) {
		t.Fatal("unthread rejected the matching fence")
	}
	if _, _, _, threaded, _ := state.Snapshot(); threaded {
		t.Fatal("web still threaded after matching unthread")
	}
	if err := state.Advance("job-a", 1); err == nil {
		t.Fatal("advance still permitted after unthread")
	}
	if !state.Unthread("job-a", 1) {
		t.Fatal("unthread is not idempotent for the matching fence")
	}
}

func TestUnthreadStaleSignalCannotTouchNewFence(t *testing.T) {
	state := NewState()
	if err := state.Fence("job-a", 1, 1); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if err := state.Thread("job-a"); err != nil {
		t.Fatalf("Thread: %v", err)
	}
	if !state.Unthread("job-a", 1) {
		t.Fatal("unthread rejected the matching fence")
	}
	if err := state.Fence("job-b", 1, 1); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if err := state.Thread("job-b"); err != nil {
		t.Fatalf("Thread: %v", err)
	}
	if state.Unthread("job-a", 1) {
		t.Fatal("stale signal unthreaded the new job's web")
	}
	if _, _, _, threaded, _ := state.Snapshot(); !threaded {
		t.Fatal("new job's web unthreaded by a stale signal")
	}
	if err := state.Advance("job-b", 1); err != nil {
		t.Fatalf("new job's advance rejected after stale signal: %v", err)
	}
}
