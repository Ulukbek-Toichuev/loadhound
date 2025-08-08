/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock and test structures

// MockStatementExecutor for testing Thread
type MockStatementExecutor struct {
	mock.Mock
	Query  string
	Pacing time.Duration
}

func (m *MockStatementExecutor) Fn(ctx context.Context) *QueryResult {
	args := m.Called(ctx)
	return args.Get(0).(*QueryResult)
}

// Tests for Thread (thread.go)
func TestNewThread(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	metric, err := NewMetric()
	require.NoError(t, err)

	executor := &StatementExecutor{
		Query:  "SELECT 1",
		Pacing: 0,
	}

	thread := NewThread(42, metric, executor, &logger)

	assert.NotNil(t, thread)
	assert.Equal(t, 42, thread.Id)
	assert.Equal(t, metric, thread.Metric)
	assert.Equal(t, executor, thread.statementExecutor)
	assert.NotNil(t, thread.logger)
}

func TestThread_RunOnDur(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should run until context cancellation", func(t *testing.T) {
		executionCount := 0
		executor := &StatementExecutor{
			Query:  "SELECT 1",
			Pacing: 0,
			Fn: func(ctx context.Context) *QueryResult {
				executionCount++
				return &QueryResult{
					RowsAffected: 1,
					ResponseTime: time.Millisecond,
					Err:          nil,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		startTime := time.Now()
		thread.RunOnDur(ctx, &wg)
		duration := time.Since(startTime)

		// Should have run for approximately the timeout duration
		assert.True(t, duration >= 50*time.Millisecond)
		assert.Less(t, duration, 100*time.Millisecond)

		// Should have executed multiple times
		assert.Greater(t, executionCount, 0)
		assert.Greater(t, metric.IterationsTotal, int64(0))
		assert.Greater(t, metric.QueriesTotal, int64(0))

		// Timing should be set
		assert.False(t, metric.StartTime.IsZero())
		assert.False(t, metric.StopTime.IsZero())
	})

	t.Run("should handle query errors", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		executor := &StatementExecutor{
			Query:  "INVALID SQL",
			Pacing: 0,
			Fn: func(ctx context.Context) *QueryResult {
				return &QueryResult{
					RowsAffected: 0,
					ResponseTime: time.Millisecond,
					Err:          assert.AnError,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		thread.RunOnDur(ctx, &wg)

		// Should have recorded errors
		assert.Greater(t, metric.ErrorsTotal, int64(0))
		assert.Greater(t, metric.ErrMap[assert.AnError.Error()], int64(0))
	})
}

func TestThread_RunOnIter(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	t.Run("should complete all iterations", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		executionCount := 0
		executor := &StatementExecutor{
			Query:  "SELECT 1",
			Pacing: 0,
			Fn: func(ctx context.Context) *QueryResult {
				executionCount++
				return &QueryResult{
					RowsAffected: 1,
					ResponseTime: time.Millisecond,
					Err:          nil,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)

		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)

		iterations := 10
		thread.RunOnIter(ctx, &wg, iterations)

		// Should have completed exactly the specified iterations
		assert.Equal(t, iterations, executionCount)
		assert.Equal(t, int64(iterations), metric.IterationsTotal)
		assert.Equal(t, int64(iterations), metric.QueriesTotal)

		// Timing should be set
		assert.False(t, metric.StartTime.IsZero())
		assert.False(t, metric.StopTime.IsZero())
	})

	t.Run("should stop early on context cancellation", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		executionCount := 0
		executor := &StatementExecutor{
			Query:  "SELECT 1",
			Pacing: 10 * time.Millisecond, // Add some delay
			Fn: func(ctx context.Context) *QueryResult {
				executionCount++
				return &QueryResult{
					RowsAffected: 1,
					ResponseTime: time.Millisecond,
					Err:          nil,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		thread.RunOnIter(ctx, &wg, 100) // Try to do 100 iterations

		// Should have completed fewer than 100 iterations due to timeout
		assert.Less(t, executionCount, 100)
		assert.Less(t, metric.IterationsTotal, int64(100))
	})

	t.Run("should handle zero iterations", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		executionCount := 0
		executor := &StatementExecutor{
			Query:  "SELECT 1",
			Pacing: 0,
			Fn: func(ctx context.Context) *QueryResult {
				executionCount++
				return &QueryResult{
					RowsAffected: 1,
					ResponseTime: time.Millisecond,
					Err:          nil,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)

		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)

		thread.RunOnIter(ctx, &wg, 0)

		// Should not have executed anything
		assert.Equal(t, 0, executionCount)
		assert.Equal(t, int64(0), metric.IterationsTotal)
		assert.Equal(t, int64(0), metric.QueriesTotal)
	})
}

func TestThread_exec(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should execute query and handle pacing", func(t *testing.T) {
		executed := false
		executor := &StatementExecutor{
			Query:  "SELECT 1",
			Pacing: 20 * time.Millisecond,
			Fn: func(ctx context.Context) *QueryResult {
				executed = true
				return &QueryResult{
					RowsAffected: 5,
					ResponseTime: 2 * time.Millisecond,
					Err:          nil,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)

		start := time.Now()
		thread.exec(context.Background())
		elapsed := time.Since(start)

		// Should have executed the query
		assert.True(t, executed)

		// Should have respected pacing
		assert.True(t, elapsed >= 20*time.Millisecond)
		assert.Less(t, elapsed, 30*time.Millisecond)

		// Should have recorded metrics
		assert.Equal(t, int64(1), metric.QueriesTotal)
		assert.Equal(t, int64(5), metric.RowsAffected)
		assert.Equal(t, int64(0), metric.ErrorsTotal)
	})

	t.Run("should handle query errors", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		executor := &StatementExecutor{
			Query:  "INVALID SQL",
			Pacing: 0,
			Fn: func(ctx context.Context) *QueryResult {
				return &QueryResult{
					RowsAffected: 0,
					ResponseTime: time.Millisecond,
					Err:          assert.AnError,
				}
			},
		}

		thread := NewThread(1, metric, executor, &logger)
		thread.exec(context.Background())

		// Should have recorded the error
		assert.Equal(t, int64(1), metric.QueriesTotal)
		assert.Equal(t, int64(1), metric.ErrorsTotal)
		assert.Equal(t, int64(1), metric.ErrMap[assert.AnError.Error()])
	})
}

// Tests for Scenarios
func TestNewScenarioDur(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	cfg := &ScenarioConfig{
		Duration: 10 * time.Second,
		Threads:  2,
	}
	metric, err := NewMetric()
	require.NoError(t, err)

	threads := []*Thread{}

	scenario := NewScenarioDur(&logger, cfg, threads, metric)

	assert.NotNil(t, scenario)
	assert.Equal(t, &logger, scenario.logger)
	assert.Equal(t, cfg, scenario.cfg)
	assert.Equal(t, threads, scenario.threads)
	assert.Equal(t, metric, scenario.Metric)
}

func TestScenarioDur_Run_WithoutRampUp(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	cfg := &ScenarioConfig{
		Duration: 50 * time.Millisecond,
		Threads:  2,
		RampUp:   0,
	}

	mainMetric, err := NewMetric()
	require.NoError(t, err)

	// Create real threads with test executors
	sharedId := NewSharedId()
	executor := &StatementExecutor{
		Query:  "SELECT 1",
		Pacing: 0,
		Fn: func(ctx context.Context) *QueryResult {
			return &QueryResult{
				RowsAffected: 1,
				ResponseTime: time.Millisecond,
				Err:          nil,
			}
		},
	}

	threads, err := InitThreads(2, sharedId, executor, &logger)
	require.NoError(t, err)

	scenario := NewScenarioDur(&logger, cfg, threads, mainMetric)

	ctx := context.Background()
	err = scenario.Run(ctx)

	assert.NoError(t, err)

	// Verify metrics were aggregated
	assert.Greater(t, mainMetric.IterationsTotal, int64(0))
	assert.Greater(t, mainMetric.QueriesTotal, int64(0))
	assert.Greater(t, mainMetric.RowsAffected, int64(0))

	// Verify timing was set
	assert.False(t, mainMetric.StartTime.IsZero())
	assert.False(t, mainMetric.StopTime.IsZero())
	assert.True(t, mainMetric.StopTime.After(mainMetric.StartTime))
}

func TestScenarioDur_Run_WithRampUp(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	cfg := &ScenarioConfig{
		Duration: 60 * time.Millisecond,
		Threads:  3,
		RampUp:   30 * time.Millisecond,
	}

	mainMetric, err := NewMetric()
	require.NoError(t, err)

	sharedId := NewSharedId()
	executor := &StatementExecutor{
		Query:  "SELECT 1",
		Pacing: 0,
		Fn: func(ctx context.Context) *QueryResult {
			return &QueryResult{
				RowsAffected: 1,
				ResponseTime: time.Millisecond,
				Err:          nil,
			}
		},
	}

	threads, err := InitThreads(3, sharedId, executor, &logger)
	require.NoError(t, err)

	scenario := NewScenarioDur(&logger, cfg, threads, mainMetric)

	ctx := context.Background()
	startTime := time.Now()
	err = scenario.Run(ctx)
	duration := time.Since(startTime)

	assert.NoError(t, err)

	// Verify ramp-up timing (should take at least the ramp-up time)
	assert.True(t, duration >= cfg.RampUp)

	// Verify metrics aggregation
	assert.Greater(t, mainMetric.IterationsTotal, int64(0))
	assert.Greater(t, mainMetric.QueriesTotal, int64(0))
}

func TestScenarioIter_Run_WithoutRampUp(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	cfg := &ScenarioConfig{
		Iterations: 5,
		Threads:    2,
		RampUp:     0,
	}

	mainMetric, err := NewMetric()
	require.NoError(t, err)

	sharedId := NewSharedId()
	executor := &StatementExecutor{
		Query:  "SELECT 1",
		Pacing: 0,
		Fn: func(ctx context.Context) *QueryResult {
			return &QueryResult{
				RowsAffected: 1,
				ResponseTime: time.Millisecond,
				Err:          nil,
			}
		},
	}

	threads, err := InitThreads(2, sharedId, executor, &logger)
	require.NoError(t, err)

	scenario := NewScenarioIter(&logger, cfg, threads, mainMetric)

	ctx := context.Background()
	err = scenario.Run(ctx)

	assert.NoError(t, err)

	// Verify metrics - should be exactly 5 iterations per thread = 10 total
	assert.Equal(t, int64(10), mainMetric.IterationsTotal)
	assert.Equal(t, int64(10), mainMetric.QueriesTotal)
	assert.Equal(t, int64(10), mainMetric.RowsAffected)
}

func TestScenarioIter_Run_WithRampUp(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	cfg := &ScenarioConfig{
		Iterations: 3,
		Threads:    2,
		RampUp:     20 * time.Millisecond,
	}

	mainMetric, err := NewMetric()
	require.NoError(t, err)

	sharedId := NewSharedId()
	executor := &StatementExecutor{
		Query:  "SELECT 1",
		Pacing: 0,
		Fn: func(ctx context.Context) *QueryResult {
			return &QueryResult{
				RowsAffected: 1,
				ResponseTime: time.Millisecond,
				Err:          nil,
			}
		},
	}

	threads, err := InitThreads(2, sharedId, executor, &logger)
	require.NoError(t, err)

	scenario := NewScenarioIter(&logger, cfg, threads, mainMetric)

	ctx := context.Background()
	startTime := time.Now()
	err = scenario.Run(ctx)
	duration := time.Since(startTime)

	assert.NoError(t, err)
	assert.True(t, duration >= cfg.RampUp)

	// Should complete exactly the specified iterations
	assert.Equal(t, int64(6), mainMetric.IterationsTotal) // 3 * 2 threads
}
