package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type Cancellation struct {
	JobID       model.JobID `json:"job_id"`
	Generation  uint64      `json:"generation"`
	CancelledAt time.Time   `json:"cancelled_at"`
	Components  []string    `json:"components"`
}

func (runtime *Runtime) CancelCurrent(ctx context.Context) (Cancellation, error) {
	current, ok := runtime.jobs.Current()
	if !ok {
		return Cancellation{}, fmt.Errorf("there is no current job")
	}
	select {
	case <-ctx.Done():
		return Cancellation{}, ctx.Err()
	default:
	}
	if err := runtime.jobControl.Cancel(current, time.Now()); err != nil {
		return Cancellation{}, err
	}
	components := []string{"job", "dryer"}
	// Stop the unwind feed for this job's roll. The owner+generation fence makes
	// repeated cancels and signals from older jobs a no-op, so a newer job's roll
	// keeps feeding.
	_, rollGeneration, _, _ := runtime.unwind.Snapshot()
	if rollGeneration == 0 {
		rollGeneration = current.RollGeneration
	}
	if runtime.unwind.StopFeed(string(current.ID), rollGeneration) {
		components = append(components, "unwind")
	}
	// Revoke the web threading qualification so further advance is rejected.
	if runtime.web.Cancel(string(current.ID), current.Generation) {
		components = append(components, "web")
	}
	return Cancellation{JobID: current.ID, Generation: current.Generation, CancelledAt: time.Now().UTC(), Components: components}, nil
}

func (runtime *Runtime) SaveSnapshot() error {
	current, ok := runtime.jobs.Current()
	if !ok {
		return fmt.Errorf("there is no current job")
	}
	owner, jobGeneration, rollGeneration, threaded, meter := runtime.web.Snapshot()
	values := map[string]any{"job": current, "web_owner": owner, "web_job_generation": jobGeneration, "web_roll_generation": rollGeneration, "web_threaded": threaded, "web_meter": meter}
	return runtimeSaveSnapshot(runtime.config.DataDir, values)
}
