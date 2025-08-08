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
)

func TestInitThreads(t *testing.T) {
	cfg := &StatementConfig{
		Name:        "test config",
		PathToQuery: "path/to/query.sql",
		Query:       "SELECT * FROM users;",
	}
	mockExecutor, err := NewStatementExecutor(context.Background(), time.Second, cfg, nil)
	if err != nil {
		return
	}
	tests := []struct {
		name    string
		threads int
		wantErr bool
	}{
		{
			name:    "successful initialization with single thread",
			threads: 1,
			wantErr: false,
		},
		{
			name:    "successful initialization with multiple threads",
			threads: 5,
			wantErr: false,
		},
		{
			name:    "successful initialization with zero threads",
			threads: 0,
			wantErr: false,
		},
		{
			name:    "successful initialization with large number of threads",
			threads: 100,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sharedId := NewSharedId()
			logger := zerolog.Nop()

			threads, err := InitThreads(tt.threads, sharedId, mockExecutor, &logger)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, threads)
			} else {
				assert.NoError(t, err)
				assert.Len(t, threads, tt.threads)

				// Verify thread IDs are sequential and unique
				expectedIds := make(map[int]bool)
				for i := 1; i <= tt.threads; i++ {
					expectedIds[i] = true
				}

				actualIds := make(map[int]bool)
				for _, thread := range threads {
					assert.NotNil(t, thread)
					assert.NotContains(t, actualIds, thread.Id, "Thread ID should be unique")
					actualIds[thread.Id] = true
					assert.Contains(t, expectedIds, thread.Id, "Thread ID should be in expected range")
				}
			}
		})
	}
}

func TestSharedId(t *testing.T) {
	t.Run("new shared id starts at zero", func(t *testing.T) {
		sharedId := NewSharedId()
		assert.NotNil(t, sharedId)
		assert.NotNil(t, sharedId.mu)
		assert.Equal(t, 0, sharedId.idx)
	})

	t.Run("get id increments and returns correctly", func(t *testing.T) {
		sharedId := NewSharedId()

		// Test sequential ID generation
		for i := 1; i <= 10; i++ {
			id := sharedId.GetId()
			assert.Equal(t, i, id)
		}
	})

	t.Run("concurrent access is thread-safe", func(t *testing.T) {
		sharedId := NewSharedId()
		numGoroutines := 100
		numCallsPerGoroutine := 10
		totalCalls := numGoroutines * numCallsPerGoroutine

		ids := make(chan int, totalCalls)
		var wg sync.WaitGroup

		// Launch multiple goroutines to test concurrent access
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numCallsPerGoroutine; j++ {
					ids <- sharedId.GetId()
				}
			}()
		}

		wg.Wait()
		close(ids)

		// Collect all IDs and verify uniqueness
		collectedIds := make(map[int]bool)
		count := 0
		for id := range ids {
			assert.False(t, collectedIds[id], "ID %d should be unique", id)
			collectedIds[id] = true
			count++
		}

		assert.Equal(t, totalCalls, count)
		assert.Len(t, collectedIds, totalCalls)

		// Verify all IDs are in the expected range [1, totalCalls]
		for i := 1; i <= totalCalls; i++ {
			assert.True(t, collectedIds[i], "ID %d should exist", i)
		}
	})

	t.Run("stress test with high concurrency", func(t *testing.T) {
		sharedId := NewSharedId()
		numGoroutines := 1000
		iterations := 100

		var wg sync.WaitGroup
		results := make(chan int, numGoroutines*iterations)

		start := time.Now()
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					results <- sharedId.GetId()
				}
			}()
		}

		wg.Wait()
		close(results)
		duration := time.Since(start)

		// Verify uniqueness and count
		seen := make(map[int]bool)
		count := 0
		for id := range results {
			assert.False(t, seen[id], "Duplicate ID found: %d", id)
			seen[id] = true
			count++
		}

		assert.Equal(t, numGoroutines*iterations, count)
		t.Logf("Stress test completed in %v with %d unique IDs", duration, count)
	})
}

func TestEvaluatePacing(t *testing.T) {
	tests := []struct {
		name           string
		pacing         time.Duration
		artificialWait time.Duration
		expectSleep    bool
		tolerance      time.Duration
	}{
		{
			name:           "zero pacing should not sleep",
			pacing:         0,
			artificialWait: 0,
			expectSleep:    false,
			tolerance:      time.Millisecond,
		},
		{
			name:           "elapsed time equals pacing should not sleep",
			pacing:         100 * time.Millisecond,
			artificialWait: 100 * time.Millisecond,
			expectSleep:    false,
			tolerance:      10 * time.Millisecond,
		},
		{
			name:           "elapsed time exceeds pacing should not sleep",
			pacing:         50 * time.Millisecond,
			artificialWait: 100 * time.Millisecond,
			expectSleep:    false,
			tolerance:      10 * time.Millisecond,
		},
		{
			name:           "elapsed time less than pacing should sleep",
			pacing:         100 * time.Millisecond,
			artificialWait: 30 * time.Millisecond,
			expectSleep:    true,
			tolerance:      15 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()

			// Simulate some elapsed time
			if tt.artificialWait > 0 {
				time.Sleep(tt.artificialWait)
			}

			pacingStart := time.Now()
			EvaluatePacing(start, tt.pacing)
			pacingDuration := time.Since(pacingStart)

			totalDuration := time.Since(start)

			if tt.expectSleep {
				// Should sleep for approximately (pacing - artificialWait)
				expectedSleepTime := tt.pacing - tt.artificialWait
				assert.True(t, pacingDuration >= expectedSleepTime-tt.tolerance,
					"Expected to sleep for at least %v, but slept for %v", expectedSleepTime-tt.tolerance, pacingDuration)
				assert.True(t, totalDuration >= tt.pacing-tt.tolerance,
					"Total duration should be at least %v, but was %v", tt.pacing-tt.tolerance, totalDuration)
			} else {
				// Should not sleep (or sleep very briefly)
				assert.True(t, pacingDuration < 10*time.Millisecond,
					"Expected minimal sleep time, but slept for %v", pacingDuration)
			}
		})
	}
}

func TestEvaluatePacing_PrecisionTest(t *testing.T) {
	t.Run("precise timing with small pacing values", func(t *testing.T) {
		pacing := 10 * time.Millisecond
		start := time.Now()

		// Sleep for less than pacing
		time.Sleep(3 * time.Millisecond)

		pacingStart := time.Now()
		EvaluatePacing(start, pacing)
		pacingDuration := time.Since(pacingStart)

		totalDuration := time.Since(start)

		// The remaining sleep should be approximately 7ms
		expectedRemaining := 7 * time.Millisecond
		tolerance := 3 * time.Millisecond

		assert.True(t, pacingDuration >= expectedRemaining-tolerance,
			"Expected pacing sleep of ~%v, got %v", expectedRemaining, pacingDuration)
		assert.True(t, totalDuration >= pacing-tolerance,
			"Total duration should be ~%v, got %v", pacing, totalDuration)
	})
}

func TestEvaluatePacing_EdgeCases(t *testing.T) {
	t.Run("negative remaining time", func(t *testing.T) {
		pacing := 50 * time.Millisecond
		start := time.Now()

		// Sleep for much longer than pacing
		time.Sleep(100 * time.Millisecond)

		pacingStart := time.Now()
		EvaluatePacing(start, pacing)
		pacingDuration := time.Since(pacingStart)

		// Should not sleep since elapsed > pacing
		assert.True(t, pacingDuration < 5*time.Millisecond,
			"Should not sleep when elapsed time exceeds pacing, but slept for %v", pacingDuration)
	})

	t.Run("very large pacing value", func(t *testing.T) {
		pacing := time.Hour
		start := time.Now()

		// Small artificial wait
		time.Sleep(time.Millisecond)

		pacingStart := time.Now()

		// Use a timeout to prevent test from hanging
		done := make(chan bool, 1)
		go func() {
			EvaluatePacing(start, pacing)
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("EvaluatePacing should not complete quickly with large pacing value")
		case <-time.After(100 * time.Millisecond):
			// This is expected - the function should still be sleeping
			// We can't easily test the full hour wait, so we just verify it started sleeping
			pacingDuration := time.Since(pacingStart)
			assert.True(t, pacingDuration >= 100*time.Millisecond,
				"Should be sleeping for large pacing value")
		}
	})
}

func TestEvaluatePacing_ConcurrentUsage(t *testing.T) {
	t.Run("concurrent pacing evaluation", func(t *testing.T) {
		pacing := 50 * time.Millisecond
		numGoroutines := 10

		var wg sync.WaitGroup
		start := time.Now()
		results := make(chan time.Duration, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Each goroutine starts at slightly different times
				localStart := time.Now()
				time.Sleep(time.Duration(id) * time.Millisecond)

				pacingStart := time.Now()
				EvaluatePacing(localStart, pacing)
				pacingDuration := time.Since(pacingStart)

				results <- pacingDuration
			}(i)
		}

		wg.Wait()
		close(results)

		totalDuration := time.Since(start)
		t.Logf("Concurrent pacing test completed in %v", totalDuration)

		// Verify each goroutine handled pacing correctly
		count := 0
		for duration := range results {
			count++
			// Each should have slept for some amount since they all had artificial waits < pacing
			assert.True(t, duration > 0, "Goroutine should have slept for some duration")
		}

		assert.Equal(t, numGoroutines, count)
	})
}

// Benchmark tests
func BenchmarkEvaluatePacing_NoPacing(b *testing.B) {
	start := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		EvaluatePacing(start, 0)
	}
}

func BenchmarkEvaluatePacing_WithPacing(b *testing.B) {
	pacing := time.Microsecond // Very small pacing to minimize test time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		EvaluatePacing(start, pacing)
	}
}
