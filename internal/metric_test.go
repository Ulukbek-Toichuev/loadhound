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

func TestNewThreadStat(t *testing.T) {
	tests := []struct {
		name      string
		wantError bool
		validate  func(t *testing.T, ts *ThreadStat)
	}{
		{
			name:      "successful creation",
			wantError: false,
			validate: func(t *testing.T, ts *ThreadStat) {
				assert.NotNil(t, ts.td)
				assert.NotNil(t, ts.errMap)
				assert.Empty(t, ts.errMap)
				assert.Equal(t, int64(0), ts.iterationsTotal)
				assert.Equal(t, int64(0), ts.rowsAffected)
				assert.Equal(t, int64(0), ts.queriesTotal)
				assert.Equal(t, int64(0), ts.errorsTotal)
				assert.True(t, ts.startTime.IsZero())
				assert.True(t, ts.stopTime.IsZero())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewThreadStat()

			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
				if tt.validate != nil {
					tt.validate(t, got)
				}
			}
		})
	}
}

func TestThreadStat_SetStartTime(t *testing.T) {
	tests := []struct {
		name      string
		startTime time.Time
		validate  func(t *testing.T, ts *ThreadStat)
	}{
		{
			name:      "set valid start time",
			startTime: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			validate: func(t *testing.T, ts *ThreadStat) {
				expected := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
				assert.Equal(t, expected, ts.startTime)
			},
		},
		{
			name:      "set zero time",
			startTime: time.Time{},
			validate: func(t *testing.T, ts *ThreadStat) {
				assert.True(t, ts.startTime.IsZero())
			},
		},
		{
			name:      "set current time",
			startTime: time.Now(),
			validate: func(t *testing.T, ts *ThreadStat) {
				assert.False(t, ts.startTime.IsZero())
				assert.WithinDuration(t, time.Now(), ts.startTime, time.Second)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := NewThreadStat()
			require.NoError(t, err)

			ts.SetStartTime(tt.startTime)

			if tt.validate != nil {
				tt.validate(t, ts)
			}
		})
	}
}

func TestThreadStat_SetStopTime(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		startTime time.Time
		stopTime  time.Time
		expected  time.Time
		shouldSet bool
	}{
		{
			name:      "valid stop time after start time",
			startTime: baseTime,
			stopTime:  baseTime.Add(10 * time.Second),
			expected:  baseTime.Add(10 * time.Second),
			shouldSet: true,
		},
		{
			name:      "stop time before start time - should not set",
			startTime: baseTime,
			stopTime:  baseTime.Add(-10 * time.Second),
			expected:  time.Time{},
			shouldSet: false,
		},
		{
			name:      "stop time equals start time",
			startTime: baseTime,
			stopTime:  baseTime,
			expected:  baseTime,
			shouldSet: true,
		},
		{
			name:      "zero start time - should set any stop time",
			startTime: time.Time{},
			stopTime:  baseTime,
			expected:  baseTime,
			shouldSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := NewThreadStat()
			require.NoError(t, err)

			if !tt.startTime.IsZero() {
				ts.SetStartTime(tt.startTime)
			}

			ts.SetStopTime(tt.stopTime)

			if tt.shouldSet {
				assert.Equal(t, tt.expected, ts.stopTime)
			} else {
				assert.True(t, ts.stopTime.IsZero())
			}
		})
	}
}

func TestThreadStat_AddIter(t *testing.T) {
	tests := []struct {
		name          string
		iterations    int
		expectedTotal int64
	}{
		{
			name:          "single iteration",
			iterations:    1,
			expectedTotal: 1,
		},
		{
			name:          "multiple iterations",
			iterations:    10,
			expectedTotal: 10,
		},
		{
			name:          "zero iterations",
			iterations:    0,
			expectedTotal: 0,
		},
		{
			name:          "large number of iterations",
			iterations:    1000,
			expectedTotal: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := NewThreadStat()
			require.NoError(t, err)

			for i := 0; i < tt.iterations; i++ {
				ts.AddIter()
			}

			assert.Equal(t, tt.expectedTotal, ts.iterationsTotal)
		})
	}
}

func TestThreadStat_SubmitQueryResult(t *testing.T) {
	tests := []struct {
		name              string
		queryResult       *QueryResult
		expectedQueries   int64
		expectedRows      int64
		expectedErrors    int64
		expectedErrMapLen int
		validate          func(t *testing.T, ts *ThreadStat)
	}{
		{
			name:              "nil query result",
			queryResult:       nil,
			expectedQueries:   0,
			expectedRows:      0,
			expectedErrors:    0,
			expectedErrMapLen: 0,
		},
		{
			name: "successful query result",
			queryResult: &QueryResult{
				RowsAffected: 5,
				ResponseTime: 100 * time.Millisecond,
				Err:          nil,
			},
			expectedQueries:   1,
			expectedRows:      5,
			expectedErrors:    0,
			expectedErrMapLen: 0,
		},
		{
			name: "query result with error",
			queryResult: &QueryResult{
				RowsAffected: 0,
				ResponseTime: 50 * time.Millisecond,
				Err:          errors.New("database connection failed"),
			},
			expectedQueries:   1,
			expectedRows:      0,
			expectedErrors:    1,
			expectedErrMapLen: 1,
			validate: func(t *testing.T, ts *ThreadStat) {
				assert.Equal(t, int64(1), ts.errMap["database connection failed"])
			},
		},
		{
			name: "multiple successful queries",
			queryResult: &QueryResult{
				RowsAffected: 3,
				ResponseTime: 75 * time.Millisecond,
				Err:          nil,
			},
			expectedQueries:   1,
			expectedRows:      3,
			expectedErrors:    0,
			expectedErrMapLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := NewThreadStat()
			require.NoError(t, err)

			ts.SubmitQueryResult(tt.queryResult)

			assert.Equal(t, tt.expectedQueries, ts.queriesTotal)
			assert.Equal(t, tt.expectedRows, ts.rowsAffected)
			assert.Equal(t, tt.expectedErrors, ts.errorsTotal)
			assert.Len(t, ts.errMap, tt.expectedErrMapLen)

			if tt.validate != nil {
				tt.validate(t, ts)
			}
		})
	}
}

func TestThreadStat_SubmitQueryResult_MultipleResults(t *testing.T) {
	ts, err := NewThreadStat()
	require.NoError(t, err)

	// Submit multiple results
	results := []*QueryResult{
		{RowsAffected: 1, ResponseTime: 10 * time.Millisecond, Err: nil},
		{RowsAffected: 2, ResponseTime: 20 * time.Millisecond, Err: errors.New("error1")},
		{RowsAffected: 3, ResponseTime: 30 * time.Millisecond, Err: nil},
		{RowsAffected: 0, ResponseTime: 40 * time.Millisecond, Err: errors.New("error1")}, // Same error
		{RowsAffected: 1, ResponseTime: 50 * time.Millisecond, Err: errors.New("error2")}, // Different error
	}

	for _, result := range results {
		ts.SubmitQueryResult(result)
	}

	assert.Equal(t, int64(5), ts.queriesTotal)
	assert.Equal(t, int64(7), ts.rowsAffected) // 1+2+3+0+1
	assert.Equal(t, int64(3), ts.errorsTotal)
	assert.Len(t, ts.errMap, 2)
	assert.Equal(t, int64(2), ts.errMap["error1"])
	assert.Equal(t, int64(1), ts.errMap["error2"])
}

func TestNewGlobalMetric(t *testing.T) {
	tests := []struct {
		name        string
		threadStats []*ThreadStat
		validate    func(t *testing.T, gm *GlobalMetric)
	}{
		{
			name:        "empty thread stats",
			threadStats: []*ThreadStat{},
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.NotNil(t, gm.Td)
				assert.NotNil(t, gm.ErrMap)
				assert.Empty(t, gm.ErrMap)
				assert.Equal(t, int64(0), gm.QueriesTotal)
				assert.Equal(t, 0.0, gm.Qps)
				assert.Equal(t, 0, gm.ThreadsTotal)
			},
		},
		{
			name:        "nil thread stats",
			threadStats: nil,
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.NotNil(t, gm.Td)
				assert.NotNil(t, gm.ErrMap)
				assert.Nil(t, gm.threadStats)
			},
		},
		{
			name: "with thread stats",
			threadStats: func() []*ThreadStat {
				ts1, _ := NewThreadStat()
				ts2, _ := NewThreadStat()
				return []*ThreadStat{ts1, ts2}
			}(),
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.NotNil(t, gm.Td)
				assert.NotNil(t, gm.ErrMap)
				assert.Len(t, gm.threadStats, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewGlobalMetric(tt.threadStats)
			require.NotNil(t, got)

			if tt.validate != nil {
				tt.validate(t, got)
			}
		})
	}
}

func TestGlobalMetric_Collect(t *testing.T) {
	tests := []struct {
		name         string
		setupThreads func() []*ThreadStat
		expectedQPS  float64
		validate     func(t *testing.T, gm *GlobalMetric)
	}{
		{
			name: "collect from single thread",
			setupThreads: func() []*ThreadStat {
				ts, _ := NewThreadStat()
				ts.SubmitQueryResult(&QueryResult{RowsAffected: 10, ResponseTime: 100 * time.Millisecond})
				ts.SubmitQueryResult(&QueryResult{RowsAffected: 5, ResponseTime: 150 * time.Millisecond})
				return []*ThreadStat{ts}
			},
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.Equal(t, int64(2), gm.QueriesTotal)
				assert.Equal(t, int64(15), gm.RowsAffectedTotal)
				assert.Equal(t, int64(0), gm.ErrorsTotal)
				assert.Empty(t, gm.ErrMap)
			},
		},
		{
			name: "collect from multiple threads",
			setupThreads: func() []*ThreadStat {
				ts1, _ := NewThreadStat()
				ts1.SubmitQueryResult(&QueryResult{RowsAffected: 1, ResponseTime: 50 * time.Millisecond})
				ts1.AddIter()

				ts2, _ := NewThreadStat()
				ts2.SubmitQueryResult(&QueryResult{RowsAffected: 2, ResponseTime: 75 * time.Millisecond, Err: errors.New("test error")})
				ts2.AddIter()
				ts2.AddIter()

				return []*ThreadStat{ts1, ts2}
			},
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.Equal(t, int64(2), gm.QueriesTotal)
				assert.Equal(t, int64(3), gm.RowsAffectedTotal)
				assert.Equal(t, int64(1), gm.ErrorsTotal)
				assert.Equal(t, int64(3), gm.IterationsTotal)
				assert.Len(t, gm.ErrMap, 1)
				assert.Equal(t, int64(1), gm.ErrMap["test error"])
			},
		},
		{
			name: "collect with nil thread stats",
			setupThreads: func() []*ThreadStat {
				return nil
			},
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.Equal(t, int64(0), gm.QueriesTotal)
				assert.Equal(t, int64(0), gm.RowsAffectedTotal)
				assert.Equal(t, int64(0), gm.ErrorsTotal)
				assert.Empty(t, gm.ErrMap)
			},
		},
		{
			name: "collect with nil thread in slice",
			setupThreads: func() []*ThreadStat {
				ts, _ := NewThreadStat()
				ts.SubmitQueryResult(&QueryResult{RowsAffected: 5, ResponseTime: 100 * time.Millisecond})
				return []*ThreadStat{ts, nil}
			},
			validate: func(t *testing.T, gm *GlobalMetric) {
				assert.Equal(t, int64(1), gm.QueriesTotal)
				assert.Equal(t, int64(5), gm.RowsAffectedTotal)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threadStats := tt.setupThreads()
			gm := NewGlobalMetric(threadStats)

			// Set time range for QPS calculation
			gm.StartAt = time.Now().Add(-1 * time.Second)
			gm.EndAt = time.Now()

			gm.Collect()

			if tt.validate != nil {
				tt.validate(t, gm)
			}
		})
	}
}

func TestGlobalMetric_GetQPS(t *testing.T) {
	tests := []struct {
		name         string
		startAt      time.Time
		endAt        time.Time
		queriesTotal int64
		expectedQPS  float64
	}{
		{
			name:         "normal case - 10 queries in 2 seconds",
			startAt:      time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			endAt:        time.Date(2025, 1, 1, 12, 0, 2, 0, time.UTC),
			queriesTotal: 10,
			expectedQPS:  5.0,
		},
		{
			name:         "zero duration",
			startAt:      time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			endAt:        time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			queriesTotal: 10,
			expectedQPS:  0.0,
		},
		{
			name:         "negative duration",
			startAt:      time.Date(2025, 1, 1, 12, 0, 2, 0, time.UTC),
			endAt:        time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			queriesTotal: 10,
			expectedQPS:  0.0,
		},
		{
			name:         "zero queries",
			startAt:      time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			endAt:        time.Date(2025, 1, 1, 12, 0, 1, 0, time.UTC),
			queriesTotal: 0,
			expectedQPS:  0.0,
		},
		{
			name:         "high QPS - 1000 queries in 1 second",
			startAt:      time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			endAt:        time.Date(2025, 1, 1, 12, 0, 1, 0, time.UTC),
			queriesTotal: 1000,
			expectedQPS:  1000.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGlobalMetric([]*ThreadStat{})
			gm.StartAt = tt.startAt
			gm.EndAt = tt.endAt
			gm.QueriesTotal = tt.queriesTotal

			got := gm.GetQPS()
			assert.Equal(t, tt.expectedQPS, got)
		})
	}
}

func TestGlobalMetric_GetSuccessRate(t *testing.T) {
	tests := []struct {
		name         string
		queriesTotal int64
		errorsTotal  int64
		expectedRate float64
	}{
		{
			name:         "100% success rate",
			queriesTotal: 100,
			errorsTotal:  0,
			expectedRate: 100.0,
		},
		{
			name:         "50% success rate",
			queriesTotal: 100,
			errorsTotal:  50,
			expectedRate: 50.0,
		},
		{
			name:         "0% success rate",
			queriesTotal: 100,
			errorsTotal:  100,
			expectedRate: 0.0,
		},
		{
			name:         "zero queries",
			queriesTotal: 0,
			errorsTotal:  0,
			expectedRate: 0.0,
		},
		{
			name:         "single successful query",
			queriesTotal: 1,
			errorsTotal:  0,
			expectedRate: 100.0,
		},
		{
			name:         "single failed query",
			queriesTotal: 1,
			errorsTotal:  1,
			expectedRate: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGlobalMetric([]*ThreadStat{})
			gm.QueriesTotal = tt.queriesTotal
			gm.ErrorsTotal = tt.errorsTotal

			got := gm.GetSuccessRate()
			assert.Equal(t, tt.expectedRate, got)
		})
	}
}

func TestGlobalMetric_GetFailedRate(t *testing.T) {
	tests := []struct {
		name         string
		queriesTotal int64
		errorsTotal  int64
		expectedRate float64
	}{
		{
			name:         "0% failure rate",
			queriesTotal: 100,
			errorsTotal:  0,
			expectedRate: 0.0,
		},
		{
			name:         "50% failure rate",
			queriesTotal: 100,
			errorsTotal:  50,
			expectedRate: 50.0,
		},
		{
			name:         "100% failure rate",
			queriesTotal: 100,
			errorsTotal:  100,
			expectedRate: 100.0,
		},
		{
			name:         "zero queries",
			queriesTotal: 0,
			errorsTotal:  0,
			expectedRate: 0.0,
		},
		{
			name:         "single successful query",
			queriesTotal: 1,
			errorsTotal:  0,
			expectedRate: 0.0,
		},
		{
			name:         "single failed query",
			queriesTotal: 1,
			errorsTotal:  1,
			expectedRate: 100.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGlobalMetric([]*ThreadStat{})
			gm.QueriesTotal = tt.queriesTotal
			gm.ErrorsTotal = tt.errorsTotal

			got := gm.GetFailedRate()
			assert.Equal(t, tt.expectedRate, got)
		})
	}
}

// Benchmark tests
func BenchmarkThreadStat_SubmitQueryResult(b *testing.B) {
	ts, _ := NewThreadStat()
	result := &QueryResult{
		RowsAffected: 1000,
		ResponseTime: 250 * time.Millisecond,
		Err:          nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.SubmitQueryResult(result)
	}
}
