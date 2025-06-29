/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package executor

import (
	"reflect"
	"testing"
	"time"
)

// thresholdDelta — допустимая погрешность (например, 10 мс)
const thresholdDelta = 10 * time.Millisecond

func TestDistributeIterations(t *testing.T) {
	tests := []struct {
		name     string
		total    int
		workers  int
		expected []int
	}{
		{
			name:     "evenly divisible",
			total:    10,
			workers:  2,
			expected: []int{5, 5},
		},
		{
			name:     "not evenly divisible",
			total:    10,
			workers:  3,
			expected: []int{4, 3, 3},
		},
		{
			name:     "single worker",
			total:    5,
			workers:  1,
			expected: []int{5},
		},
		{
			name:     "more workers than iterations",
			total:    3,
			workers:  5,
			expected: []int{1, 1, 1, 0, 0},
		},
		{
			name:     "zero iterations",
			total:    0,
			workers:  3,
			expected: []int{0, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := distributeIterations(tt.total, tt.workers)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("distributeIterations(%d, %d) = %v, want %v", tt.total, tt.workers, got, tt.expected)
			}
		})
	}
}

func TestPacing(t *testing.T) {
	tests := []struct {
		name           string
		pacingDuration time.Duration
		fakeWorkTime   time.Duration
		expectSleep    bool
	}{
		{
			name:           "should sleep when work is faster than pacing",
			pacingDuration: 100 * time.Millisecond,
			fakeWorkTime:   20 * time.Millisecond,
			expectSleep:    true,
		},
		{
			name:           "should not sleep when work equals pacing",
			pacingDuration: 50 * time.Millisecond,
			fakeWorkTime:   50 * time.Millisecond,
			expectSleep:    false,
		},
		{
			name:           "should not sleep when work is slower than pacing",
			pacingDuration: 30 * time.Millisecond,
			fakeWorkTime:   80 * time.Millisecond,
			expectSleep:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()

			// эмулируем работу
			time.Sleep(tt.fakeWorkTime)

			// вызываем pacing
			pacing(start, tt.pacingDuration)

			totalElapsed := time.Since(start)

			if tt.expectSleep {
				if totalElapsed < tt.pacingDuration-thresholdDelta {
					t.Errorf("Expected to sleep at least %v, but slept only %v", tt.pacingDuration, totalElapsed)
				}
			} else {
				if totalElapsed > tt.fakeWorkTime+thresholdDelta {
					t.Errorf("Expected no sleep, but pacing added delay: total %v (work was %v)", totalElapsed, tt.fakeWorkTime)
				}
			}
		})
	}
}
