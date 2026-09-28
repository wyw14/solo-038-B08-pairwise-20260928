package unwind

import "testing"

func TestStopFeedFenceStopsOnlyMatchingOwnerAndGeneration(t *testing.T) {
	state := NewState()

	if _, err := state.LoadRoll("roll-a"); err != nil {
		t.Fatalf("load roll: %v", err)
	}
	if err := state.StartFeed("job-a", 1, 8); err != nil {
		t.Fatalf("start feed: %v", err)
	}

	// A stale signal (wrong generation, e.g. an older roll) must not stop the feed.
	if state.StopFeed("job-a", 2) {
		t.Fatal("stale generation signal must not stop the feed")
	}
	if _, _, owner, speed := state.Snapshot(); owner != "job-a" || speed != 8 {
		t.Fatalf("feed changed by stale generation: owner=%q speed=%v", owner, speed)
	}

	// A signal from a different owner (an older job) must not stop this feed.
	if state.StopFeed("job-old", 1) {
		t.Fatal("signal from another owner must not stop the feed")
	}
	if _, _, owner, speed := state.Snapshot(); owner != "job-a" || speed != 8 {
		t.Fatalf("feed changed by foreign owner: owner=%q speed=%v", owner, speed)
	}

	// The matching owner+generation stops the feed promptly.
	if !state.StopFeed("job-a", 1) {
		t.Fatal("matching cancel must stop the feed")
	}
	if _, _, owner, speed := state.Snapshot(); owner != "" || speed != 0 {
		t.Fatalf("feed not stopped: owner=%q speed=%v", owner, speed)
	}

	// A repeated cancel is a harmless no-op.
	if state.StopFeed("job-a", 1) {
		t.Fatal("repeated cancel must be a no-op")
	}
}

func TestStopFeedDoesNotTouchNewOwnersRoll(t *testing.T) {
	state := NewState()

	if _, err := state.LoadRoll("roll-a"); err != nil {
		t.Fatalf("load roll a: %v", err)
	}
	if err := state.StartFeed("job-a", 1, 8); err != nil {
		t.Fatalf("start feed a: %v", err)
	}

	// A new job loads a fresh roll (generation bumps) and starts feeding.
	if _, err := state.LoadRoll("roll-b"); err != nil {
		t.Fatalf("load roll b: %v", err)
	}
	if err := state.StartFeed("job-b", 2, 10); err != nil {
		t.Fatalf("start feed b: %v", err)
	}

	// The old job's cancel signal must not stop the new job's roll.
	if state.StopFeed("job-a", 1) {
		t.Fatal("old job cancel must not affect the new owner")
	}
	if _, generation, owner, speed := state.Snapshot(); generation != 2 || owner != "job-b" || speed != 10 {
		t.Fatalf("new job feed disturbed: gen=%d owner=%q speed=%v", generation, owner, speed)
	}
}
