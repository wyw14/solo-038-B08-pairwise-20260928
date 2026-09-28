package web

import (
	"fmt"
	"sync"
)

type State struct {
	mu             sync.RWMutex
	owner          string
	jobGeneration  uint64
	rollGeneration uint64
	threaded       bool
	meter          float64
}

func NewState() *State {
	return &State{}
}

func (state *State) Fence(owner string, jobGeneration, rollGeneration uint64) error {
	if owner == "" || jobGeneration == 0 || rollGeneration == 0 {
		return fmt.Errorf("web fence identity is incomplete")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.owner = owner
	state.jobGeneration = jobGeneration
	state.rollGeneration = rollGeneration
	state.threaded = false
	state.meter = 0
	return nil
}

func (state *State) Thread(owner string) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if owner != state.owner {
		return fmt.Errorf("web owner fence rejected")
	}
	state.threaded = true
	return nil
}

func (state *State) Unthread(owner string, jobGeneration uint64) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if owner == "" || owner != state.owner || jobGeneration != state.jobGeneration {
		return false
	}
	state.threaded = false
	return true
}

func (state *State) Advance(owner string, meters float64) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if owner != state.owner || !state.threaded || meters < 0 {
		return fmt.Errorf("web advance is not permitted")
	}
	state.meter += meters
	return nil
}

func (state *State) Snapshot() (string, uint64, uint64, bool, float64) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.owner, state.jobGeneration, state.rollGeneration, state.threaded, state.meter
}
