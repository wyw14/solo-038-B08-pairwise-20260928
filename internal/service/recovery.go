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
	components := append([]string{"job", "dryer"}, runtime.releaseFeed(current)...)
	return Cancellation{JobID: current.ID, Generation: current.Generation, CancelledAt: time.Now().UTC(), Components: components}, nil
}

// releaseFeed stops the unwind feed and revokes web-threaded qualification for
// the given job. Every action is fenced by the job identity captured by the
// cancel signal, so a duplicate signal or one aimed at a superseded job cannot
// tear down the roll material owned by the current job.
func (runtime *Runtime) releaseFeed(job model.Job) []string {
	var components []string
	if runtime.unwind.StopFeed(string(job.ID)) {
		components = append(components, "unwind")
	}
	if runtime.web.Unthread(string(job.ID), job.Generation) {
		components = append(components, "web")
	}
	return components
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
