package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/model"
)

type Coordinator struct {
	mu          sync.Mutex
	state       *State
	dryer       *dryer.State
	dryerCancel context.CancelFunc
	dryerDone   <-chan error
}

func NewCoordinator(state *State, dryerState *dryer.State) *Coordinator {
	return &Coordinator{state: state, dryer: dryerState}
}

func (coordinator *Coordinator) StartDryerRamp(parent context.Context, job model.Job, target float64, interval time.Duration) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.dryer.Start(string(job.ID), job.Generation, target, "production", time.Now()); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	coordinator.dryerCancel = cancel
	coordinator.dryerDone = dryer.Ramp(ctx, coordinator.dryer, string(job.ID), job.Generation, target, interval, func(value float64) error {
		return coordinator.dryer.Apply(string(job.ID), job.Generation, value, time.Now())
	})
	return nil
}

func (coordinator *Coordinator) Cancel(job model.Job, now time.Time) error {
	coordinator.mu.Lock()
	cancel := coordinator.dryerCancel
	done := coordinator.dryerDone
	coordinator.mu.Unlock()
	if current, ok := coordinator.state.Current(); !ok || current.ID != job.ID || current.Generation != job.Generation {
		return fmt.Errorf("cancel signal is stale and no longer matches the current job")
	}
	updated, err := job.Transition(model.JobCancelled, now)
	if err != nil {
		return err
	}
	if err := coordinator.state.Put(updated); err != nil {
		return err
	}
	if cancel != nil {
		cancel()
	}
	coordinator.dryer.Cancel(string(job.ID), job.Generation)
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Second):
			return fmt.Errorf("dryer ramp did not stop")
		}
	}
	return nil
}

func (coordinator *Coordinator) DryerTemperature() float64 {
	_, _, _, current, _, _ := coordinator.dryer.Snapshot()
	return current
}
