/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Thread struct {
	Id       int
	Metric   *LocalMetric
	ExecFunc ExecFunc
	Pacing   time.Duration
	Query    string
	Logger   *zerolog.Logger
}

func NewThread(id int, metric *LocalMetric, execFunc ExecFunc, pacing time.Duration, query string, logger *zerolog.Logger) *Thread {
	threadLogger := logger.With().Int("thread_id", id).Logger()
	return &Thread{
		Id:       id,
		Metric:   metric,
		ExecFunc: execFunc,
		Pacing:   pacing,
		Query:    query,
		Logger:   &threadLogger,
	}
}

func InitThreads(threads int, globalId *Id, execFunc ExecFunc, pacing time.Duration, query string, logger *zerolog.Logger, localMetricRec chan *LocalMetric) ([]*Thread, error) {
	var preparedThreads = make([]*Thread, 0, threads)
	for i := 0; i < threads; i++ {
		m, err := NewLocalMetric()
		if err != nil {
			return nil, err
		}
		localMetricRec <- m
		preparedThreads = append(preparedThreads, NewThread(globalId.GetId(), m, execFunc, pacing, query, logger))
	}
	logger.Debug().Int("threads_initialized", threads).Str("pacing", pacing.String()).Msg("Threads initialized successfully")
	return preparedThreads, nil
}

func (t *Thread) RunOnDur(ctx context.Context, wg *sync.WaitGroup, startAt time.Time) {
	defer wg.Done()
	defer t.Metric.Stop()
	t.Metric.StartAt(startAt)
	t.Logger.Debug().Time("start_time", startAt).Msg("Thread started (duration-based)")

	executionCount := 0
	for {
		select {
		case <-ctx.Done():
			t.Logger.Debug().Int("executions_completed", executionCount).Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		executionCount++
		t.Metric.AddIters()

		if executionCount%100 == 0 {
			t.Logger.Debug().Int("executions_completed", executionCount).Msg("Thread execution progress")
		}
	}
}

func (t *Thread) RunOnIter(ctx context.Context, wg *sync.WaitGroup, startAt time.Time, iterations int) {
	defer wg.Done()
	defer t.Metric.Stop()
	t.Metric.StartAt(startAt)
	t.Logger.Debug().Time("start_time", startAt).Int("total_iterations", iterations).Msg("Thread started (iteration-based)")

	for iter := 0; iter < iterations; iter++ {
		select {
		case <-ctx.Done():
			t.Logger.Debug().Int("completed_iterations", iter).Int("total_iterations", iterations).Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		t.Metric.AddIters()

		if iterations >= 10 && (iter+1)%(iterations/10) == 0 {
			t.Logger.Debug().Int("completed_iterations", iter+1).Int("total_iterations", iterations).Float64("progress_percent", float64(iter+1)/float64(iterations)*100).Msg("Thread iteration progress")
		}
	}
	t.Logger.Debug().Int("completed_iterations", iterations).Msg("Thread completed all iterations")
}

func (t *Thread) exec(ctx context.Context) {
	start := time.Now()
	qm := t.ExecFunc(ctx, t.Query)
	t.Metric.Submit(qm)
	if qm.Err != nil {
		t.Logger.Error().Err(qm.Err).Str("duration", qm.ResponseTime.String()).Str("query", t.Query).Msg("Query execution failed")
	}
	t.Logger.Trace().Str("duration", qm.ResponseTime.String()).Msg("Query executed successfully")
	evaluatePacing(start, t.Pacing)
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
