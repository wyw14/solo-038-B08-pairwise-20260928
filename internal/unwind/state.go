package unwind

import (
	"fmt"
	"sync"
)

type State struct {
	mu             sync.RWMutex
	activeRoll     string
	rollGeneration uint64
	feedOwner      string
	feedSpeed      float64
}

func NewState() *State {
	return &State{}
}

func (state *State) LoadRoll(roll string) (uint64, error) {
	if roll == "" {
		return 0, fmt.Errorf("roll identity is required")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.activeRoll = roll
	state.rollGeneration++
	state.feedSpeed = 0
	return state.rollGeneration, nil
}

func (state *State) StartFeed(owner string, generation uint64, speed float64) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if owner == "" || generation != state.rollGeneration {
		return fmt.Errorf("feed fence does not match loaded roll")
	}
	if speed < 0 {
		return fmt.Errorf("feed speed cannot be negative")
	}
	state.feedOwner = owner
	state.feedSpeed = speed
	return nil
}

func (state *State) StopFeed(owner string, generation uint64) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if owner != state.feedOwner || generation != state.rollGeneration {
		return false
	}
	state.feedSpeed = 0
	state.feedOwner = ""
	return true
}

func (state *State) Snapshot() (string, uint64, string, float64) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.activeRoll, state.rollGeneration, state.feedOwner, state.feedSpeed
}
