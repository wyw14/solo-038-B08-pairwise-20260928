package web

import "testing"

func TestCancelRevokesThreadingAndBlocksAdvance(t *testing.T) {
	state := NewState()
	if err := state.Fence("job-a", 1, 1); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if err := state.Thread("job-a"); err != nil {
		t.Fatalf("thread: %v", err)
	}
	if err := state.Advance("job-a", 1); err != nil {
		t.Fatalf("advance before cancel: %v", err)
	}

	if !state.Cancel("job-a", 1) {
		t.Fatal("matching cancel must revoke threading")
	}
	if _, _, _, threaded, _ := state.Snapshot(); threaded {
		t.Fatal("web path must no longer be threaded after cancel")
	}
	// Subsequent web feed is no longer permitted.
	if err := state.Advance("job-a", 1); err == nil {
		t.Fatal("advance must be rejected after threading qualification was revoked")
	}

	// A repeated cancel is a no-op.
	if state.Cancel("job-a", 1) {
		t.Fatal("repeated cancel must be a no-op")
	}
}

func TestCancelFenceIgnoresStaleAndForeignSignals(t *testing.T) {
	state := NewState()
	if err := state.Fence("job-b", 2, 2); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if err := state.Thread("job-b"); err != nil {
		t.Fatalf("thread: %v", err)
	}

	// Old job generation's cancel must not revoke the current path.
	if state.Cancel("job-b", 1) {
		t.Fatal("stale generation cancel must be a no-op")
	}
	// A different owner's cancel must not revoke the current path.
	if state.Cancel("job-a", 2) {
		t.Fatal("foreign owner cancel must be a no-op")
	}
	if _, _, _, threaded, _ := state.Snapshot(); !threaded {
		t.Fatal("new job threading must remain intact against old cancel signals")
	}
	if err := state.Advance("job-b", 1); err != nil {
		t.Fatalf("new job must still be allowed to advance: %v", err)
	}
}
