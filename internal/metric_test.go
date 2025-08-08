/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetric(t *testing.T) {
	t.Run("should create new metric successfully", func(t *testing.T) {
		metric, err := NewMetric()

		require.NoError(t, err)
		assert.NotNil(t, metric)
		assert.NotNil(t, metric.mu)
		assert.NotNil(t, metric.Td)
		assert.NotNil(t, metric.ErrMap)
		assert.Equal(t, int64(0), metric.IterationsTotal)
		assert.Equal(t, int64(0), metric.ThreadsTotal)
		assert.Equal(t, int64(0), metric.RowsAffected)
		assert.Equal(t, int64(0), metric.QueriesTotal)
		assert.Equal(t, int64(0), metric.ErrorsTotal)
		assert.True(t, metric.StartTime.IsZero())
		assert.True(t, metric.StopTime.IsZero())
		assert.Empty(t, metric.ErrMap)
	})
}

func TestMetric_SetStartTime(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	testTime := time.Now()

	t.Run("should set start time", func(t *testing.T) {
		metric.SetStartTime(testTime)

		assert.Equal(t, testTime, metric.StartTime)
	})

	t.Run("should update start time if called multiple times", func(t *testing.T) {
		newTime := testTime.Add(time.Hour)
		metric.SetStartTime(newTime)

		assert.Equal(t, newTime, metric.StartTime)
	})
}

func TestMetric_SetStopTime(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	startTime := time.Now()
	metric.SetStartTime(startTime)

	t.Run("should set stop time when after start time", func(t *testing.T) {
		stopTime := startTime.Add(time.Minute)
		metric.SetStopTime(stopTime)

		assert.Equal(t, stopTime, metric.StopTime)
	})

	t.Run("should not set stop time when before start time", func(t *testing.T) {
		originalStopTime := metric.StopTime
		beforeStartTime := startTime.Add(-time.Minute)
		metric.SetStopTime(beforeStartTime)

		assert.Equal(t, originalStopTime, metric.StopTime)
		assert.NotEqual(t, beforeStartTime, metric.StopTime)
	})

	t.Run("should set stop time when start time is zero", func(t *testing.T) {
		metricWithoutStart, err := NewMetric()
		require.NoError(t, err)

		stopTime := time.Now()
		metricWithoutStart.SetStopTime(stopTime)

		assert.Equal(t, stopTime, metricWithoutStart.StopTime)
	})

	t.Run("should set stop time equal to start time", func(t *testing.T) {
		metric.SetStopTime(startTime)

		assert.Equal(t, startTime, metric.StopTime)
	})
}

func TestMetric_AddIter(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should increment iterations counter", func(t *testing.T) {
		initialCount := metric.IterationsTotal

		metric.AddIter()

		assert.Equal(t, initialCount+1, metric.IterationsTotal)
	})

	t.Run("should increment multiple times", func(t *testing.T) {
		initialCount := metric.IterationsTotal
		iterations := 5

		for i := 0; i < iterations; i++ {
			metric.AddIter()
		}

		assert.Equal(t, initialCount+int64(iterations), metric.IterationsTotal)
	})
}

func TestMetric_AddThread(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should increment threads counter", func(t *testing.T) {
		initialCount := metric.ThreadsTotal

		metric.AddThread()

		assert.Equal(t, initialCount+1, metric.ThreadsTotal)
	})

	t.Run("should increment multiple times", func(t *testing.T) {
		initialCount := metric.ThreadsTotal
		threads := 3

		for i := 0; i < threads; i++ {
			metric.AddThread()
		}

		assert.Equal(t, initialCount+int64(threads), metric.ThreadsTotal)
	})
}

func TestMetric_SubmitQueryResult(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should handle nil query result", func(t *testing.T) {
		err := metric.SubmitQueryResult(nil)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), metric.QueriesTotal)
		assert.Equal(t, int64(0), metric.ErrorsTotal)
		assert.Equal(t, int64(0), metric.RowsAffected)
	})

	t.Run("should process successful query result", func(t *testing.T) {
		queryResult := &QueryResult{
			RowsAffected: 5,
			ResponseTime: 100 * time.Millisecond,
			Err:          nil,
		}

		err := metric.SubmitQueryResult(queryResult)

		assert.NoError(t, err)
		assert.Equal(t, int64(1), metric.QueriesTotal)
		assert.Equal(t, int64(0), metric.ErrorsTotal)
		assert.Equal(t, int64(5), metric.RowsAffected)
		assert.Empty(t, metric.ErrMap)
	})

	t.Run("should process failed query result", func(t *testing.T) {
		testErr := errors.New("connection timeout")
		queryResult := &QueryResult{
			RowsAffected: 0,
			ResponseTime: 50 * time.Millisecond,
			Err:          testErr,
		}

		err := metric.SubmitQueryResult(queryResult)

		assert.NoError(t, err)
		assert.Equal(t, int64(2), metric.QueriesTotal) // Previous test added 1
		assert.Equal(t, int64(1), metric.ErrorsTotal)
		assert.Equal(t, int64(5), metric.RowsAffected) // Same as previous
		assert.Equal(t, int64(1), metric.ErrMap[testErr.Error()])
	})

	t.Run("should accumulate multiple errors of same type", func(t *testing.T) {
		testErr := errors.New("connection timeout")
		queryResult := &QueryResult{
			RowsAffected: 2,
			ResponseTime: 75 * time.Millisecond,
			Err:          testErr,
		}

		err := metric.SubmitQueryResult(queryResult)

		assert.NoError(t, err)
		assert.Equal(t, int64(3), metric.QueriesTotal)
		assert.Equal(t, int64(2), metric.ErrorsTotal)
		assert.Equal(t, int64(7), metric.RowsAffected)
		assert.Equal(t, int64(2), metric.ErrMap[testErr.Error()])
	})

	t.Run("should handle different error types", func(t *testing.T) {
		differentErr := errors.New("syntax error")
		queryResult := &QueryResult{
			RowsAffected: 1,
			ResponseTime: 25 * time.Millisecond,
			Err:          differentErr,
		}

		err := metric.SubmitQueryResult(queryResult)

		assert.NoError(t, err)
		assert.Equal(t, int64(4), metric.QueriesTotal)
		assert.Equal(t, int64(3), metric.ErrorsTotal)
		assert.Equal(t, int64(8), metric.RowsAffected)
		assert.Equal(t, int64(2), metric.ErrMap["connection timeout"])
		assert.Equal(t, int64(1), metric.ErrMap[differentErr.Error()])
	})
}

func TestMetric_GetSnapshot(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	// Setup some data
	startTime := time.Now()
	stopTime := startTime.Add(time.Minute)
	metric.SetStartTime(startTime)
	metric.SetStopTime(stopTime)
	metric.AddIter()
	metric.AddThread()

	queryResult := &QueryResult{
		RowsAffected: 10,
		ResponseTime: 100 * time.Millisecond,
		Err:          errors.New("test error"),
	}
	err = metric.SubmitQueryResult(queryResult)
	require.NoError(t, err)

	t.Run("should create accurate snapshot", func(t *testing.T) {
		snapshot := metric.GetSnapshot()

		assert.NotNil(t, snapshot)
		assert.Equal(t, metric.StartTime, snapshot.StartTime)
		assert.Equal(t, metric.StopTime, snapshot.StopTime)
		assert.Equal(t, metric.IterationsTotal, snapshot.IterationsTotal)
		assert.Equal(t, metric.RowsAffected, snapshot.RowsAffected)
		assert.Equal(t, metric.QueriesTotal, snapshot.QueriesTotal)
		assert.Equal(t, metric.ErrorsTotal, snapshot.ErrorsTotal)

		// Verify error map is copied
		assert.Equal(t, len(metric.ErrMap), len(snapshot.ErrMap))
		for k, v := range metric.ErrMap {
			assert.Equal(t, v, snapshot.ErrMap[k])
		}

		// Verify TDigest is cloned (not the same instance)
		assert.NotSame(t, metric.Td, snapshot.Td)
	})

	t.Run("snapshot should be independent", func(t *testing.T) {
		snapshot := metric.GetSnapshot()
		originalQueries := snapshot.QueriesTotal

		// Modify original metric
		metric.AddIter()
		successResult := &QueryResult{
			RowsAffected: 5,
			ResponseTime: 50 * time.Millisecond,
			Err:          nil,
		}
		err = metric.SubmitQueryResult(successResult)
		require.NoError(t, err)

		// Snapshot should remain unchanged
		assert.Equal(t, originalQueries, snapshot.QueriesTotal)
		assert.NotEqual(t, metric.QueriesTotal, snapshot.QueriesTotal)
	})
}

func TestMetric_GetQPS(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should return 0 when no time duration", func(t *testing.T) {
		qps := metric.GetQPS()

		assert.Equal(t, float64(0), qps)
	})

	t.Run("should return 0 when stop time before start time", func(t *testing.T) {
		now := time.Now()
		metric.SetStartTime(now)
		metric.SetStopTime(now.Add(-time.Second))

		qps := metric.GetQPS()

		assert.Equal(t, float64(0), qps)
	})

	t.Run("should calculate QPS correctly", func(t *testing.T) {
		startTime := time.Now()
		metric.SetStartTime(startTime)
		metric.SetStopTime(startTime.Add(10 * time.Second))

		// Add some queries
		for i := 0; i < 50; i++ {
			queryResult := &QueryResult{
				RowsAffected: 1,
				ResponseTime: 10 * time.Millisecond,
				Err:          nil,
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		qps := metric.GetQPS()

		assert.Equal(t, float64(5), qps) // 50 queries / 10 seconds = 5 QPS
	})

	t.Run("should handle fractional QPS", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		startTime := time.Now()
		metric.SetStartTime(startTime)
		metric.SetStopTime(startTime.Add(3 * time.Second))

		// Add 10 queries
		for i := 0; i < 10; i++ {
			queryResult := &QueryResult{
				RowsAffected: 1,
				ResponseTime: 10 * time.Millisecond,
				Err:          nil,
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		qps := metric.GetQPS()

		assert.InDelta(t, 3.333333, qps, 0.0001) // 10 queries / 3 seconds ≈ 3.33 QPS
	})
}

func TestMetric_GetSuccessRate(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should return 0 when no queries", func(t *testing.T) {
		rate := metric.GetSuccessRate()

		assert.Equal(t, float64(0), rate)
	})

	t.Run("should return 100% for all successful queries", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			queryResult := &QueryResult{
				RowsAffected: 1,
				ResponseTime: 10 * time.Millisecond,
				Err:          nil,
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		rate := metric.GetSuccessRate()

		assert.Equal(t, float64(100), rate)
	})

	t.Run("should calculate correct success rate with mixed results", func(t *testing.T) {
		// Add 5 failed queries
		for i := 0; i < 5; i++ {
			queryResult := &QueryResult{
				RowsAffected: 0,
				ResponseTime: 10 * time.Millisecond,
				Err:          errors.New("error"),
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		rate := metric.GetSuccessRate()

		// 10 successful + 5 failed = 15 total
		// 10 successful / 15 total * 100 = 66.67%
		assert.InDelta(t, 66.666666, rate, 0.0001)
	})

	t.Run("should return 0% for all failed queries", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		for i := 0; i < 5; i++ {
			queryResult := &QueryResult{
				RowsAffected: 0,
				ResponseTime: 10 * time.Millisecond,
				Err:          errors.New("error"),
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		rate := metric.GetSuccessRate()

		assert.Equal(t, float64(0), rate)
	})
}

func TestMetric_GetFailedRate(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should return 0 when no queries", func(t *testing.T) {
		rate := metric.GetFailedRate()

		assert.Equal(t, float64(0), rate)
	})

	t.Run("should return 0% for all successful queries", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			queryResult := &QueryResult{
				RowsAffected: 1,
				ResponseTime: 10 * time.Millisecond,
				Err:          nil,
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		rate := metric.GetFailedRate()

		assert.Equal(t, float64(0), rate)
	})

	t.Run("should calculate correct failure rate with mixed results", func(t *testing.T) {
		// Add 5 failed queries to the 10 successful ones
		for i := 0; i < 5; i++ {
			queryResult := &QueryResult{
				RowsAffected: 0,
				ResponseTime: 10 * time.Millisecond,
				Err:          errors.New("error"),
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		rate := metric.GetFailedRate()

		// 5 failed / 15 total * 100 = 33.33%
		assert.InDelta(t, 33.333333, rate, 0.0001)
	})

	t.Run("should return 100% for all failed queries", func(t *testing.T) {
		metric, err := NewMetric()
		require.NoError(t, err)

		for i := 0; i < 5; i++ {
			queryResult := &QueryResult{
				RowsAffected: 0,
				ResponseTime: 10 * time.Millisecond,
				Err:          errors.New("error"),
			}
			err = metric.SubmitQueryResult(queryResult)
			require.NoError(t, err)
		}

		rate := metric.GetFailedRate()

		assert.Equal(t, float64(100), rate)
	})
}

func TestMetric_ConcurrencySafety(t *testing.T) {
	metric, err := NewMetric()
	require.NoError(t, err)

	t.Run("should handle concurrent operations safely", func(t *testing.T) {
		const numGoroutines = 10
		const operationsPerGoroutine = 100

		done := make(chan bool, numGoroutines)

		// Start multiple goroutines performing various operations
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer func() { done <- true }()

				for j := 0; j < operationsPerGoroutine; j++ {
					metric.AddIter()
					metric.AddThread()

					queryResult := &QueryResult{
						RowsAffected: 1,
						ResponseTime: time.Duration(j) * time.Millisecond,
						Err:          nil,
					}
					if j%10 == 0 {
						queryResult.Err = errors.New("test error")
					}

					_ = metric.SubmitQueryResult(queryResult)
				}
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify final counts
		assert.Equal(t, int64(numGoroutines*operationsPerGoroutine), metric.IterationsTotal)
		assert.Equal(t, int64(numGoroutines*operationsPerGoroutine), metric.ThreadsTotal)
		assert.Equal(t, int64(numGoroutines*operationsPerGoroutine), metric.QueriesTotal)

		// Verify error tracking
		expectedErrors := int64(numGoroutines * (operationsPerGoroutine / 10)) // Every 10th operation fails
		assert.Equal(t, expectedErrors, metric.ErrorsTotal)
		assert.Equal(t, expectedErrors, metric.ErrMap["test error"])

		// Test concurrent snapshots
		snapshot1 := metric.GetSnapshot()
		snapshot2 := metric.GetSnapshot()

		assert.Equal(t, snapshot1.QueriesTotal, snapshot2.QueriesTotal)
		assert.Equal(t, snapshot1.ErrorsTotal, snapshot2.ErrorsTotal)
	})
}
