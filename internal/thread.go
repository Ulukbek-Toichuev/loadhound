/*
LoadHound — Relentless load testing tool for SQL databases.
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
	Id                int
	statPool          *ThreadStat
	statementExecutor *StatementExecutor
	logger            *zerolog.Logger
}

func NewThread(id int, statPool *ThreadStat, statementExecutor *StatementExecutor, logger *zerolog.Logger) *Thread {
	threadLogger := logger.With().Int("thread_id", id).Logger()
	return &Thread{
		Id:                id,
		statPool:          statPool,
		statementExecutor: statementExecutor,
		logger:            &threadLogger,
	}
}

func (t *Thread) RunOnDur(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		t.statPool.SetStopTime(time.Now())
	}()

	startTime := time.Now()
	t.statPool.SetStartTime(startTime)
	t.logger.Debug().Time("start_time", startTime).Msg("Thread started (duration-based)")

	executionCount := 0
	for {
		select {
		case <-ctx.Done():
			t.logger.Debug().Int("executions_completed", executionCount).Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		executionCount++
		t.statPool.AddIter()

		if executionCount%100 == 0 {
			t.logger.Debug().Int("executions_completed", executionCount).Msg("Thread execution progress")
		}
	}
}

func (t *Thread) RunOnIter(ctx context.Context, wg *sync.WaitGroup, iterations int) {
	defer func() {
		wg.Done()
		t.statPool.SetStopTime(time.Now())
	}()

	startTime := time.Now()
	t.statPool.SetStartTime(startTime)
	t.logger.Debug().Time("start_time", startTime).Int("total_iterations", iterations).Msg("Thread started (iteration-based)")

	for iter := 0; iter < iterations; iter++ {
		select {
		case <-ctx.Done():
			t.logger.Debug().Int("completed_iterations", iter).Int("total_iterations", iterations).Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		t.statPool.AddIter()

		if iterations >= 10 && (iter+1)%(iterations/10) == 0 {
			t.logger.Debug().Int("completed_iterations", iter+1).Int("total_iterations", iterations).Float64("progress_percent", float64(iter+1)/float64(iterations)*100).Msg("Thread iteration progress")
		}
	}
	t.logger.Debug().Int("completed_iterations", iterations).Msg("Thread completed all iterations")
}

func (t *Thread) exec(ctx context.Context) {
	start := time.Now()
	queryResult := t.statementExecutor.fn(ctx)
	t.statPool.SubmitQueryResult(queryResult)
	if queryResult.Err != nil {
		t.logger.Error().Err(queryResult.Err).Str("duration", queryResult.ResponseTime.String()).Str("query", t.statementExecutor.cfg.Query).Msg("Query execution failed")
	}
	t.logger.Trace().Str("duration", queryResult.ResponseTime.String()).Msg("Query executed successfully")
	EvaluatePacing(start, t.statementExecutor.pacing)
}
