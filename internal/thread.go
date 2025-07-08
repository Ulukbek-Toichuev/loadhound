package internal

import (
	"context"
	"sync"
	"time"
)

type Thread struct {
	Id              int
	localMetric     *LocalMetric
	Task            func(ctx context.Context) *QueryMetric
	workflowCfg     *WorkflowConfig
	eventController *GeneralEventController
}

func NewThread(id int, cfg *WorkflowConfig, eventController *GeneralEventController, fn func(globalCtx context.Context) *QueryMetric) *Thread {
	l, err := NewLocalMetric()
	if err != nil {
		return nil
	}
	return &Thread{Id: id, localMetric: l, workflowCfg: cfg, eventController: eventController, Task: fn}
}

func (t *Thread) ExecByDur(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	t.localMetric.SetStartTIme()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.exec(ctx)
	}
}

func (t *Thread) ExecByIter(globalCtx context.Context, wg *sync.WaitGroup, iterations int) {
	defer wg.Done()
	t.localMetric.SetStartTIme()

	for i := 0; i < iterations; i++ {
		select {
		case <-globalCtx.Done():
			return
		default:
		}
		t.exec(globalCtx)
	}
}

func (t *Thread) exec(ctx context.Context) {
	pacing := t.workflowCfg.Pacing
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	q := t.Task(ctx)
	q.ThreadId = t.Id
	t.localMetric.SetQueryMetric(q)

	evaluatePacing(start, pacing)
}

func evaluatePacing(start time.Time, pacing time.Duration) {
	if pacing == 0 {
		return
	}
	elapsed := time.Since(start)
	if elapsed >= pacing {
		return
	}
	remaining := pacing - elapsed
	if remaining > 0 {
		time.Sleep(remaining)
	}
}
