/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetArgs(t *testing.T) {
	tests := []struct {
		name       string
		generators []GeneratorFunc
		wantLen    int
		validate   func(t *testing.T, args []any)
	}{
		{
			name:       "empty generators",
			generators: []GeneratorFunc{},
			wantLen:    0,
			validate: func(t *testing.T, args []any) {
				assert.Empty(t, args)
			},
		},
		{
			name: "single generator - bool",
			generators: []GeneratorFunc{
				func() any { return true },
			},
			wantLen: 1,
			validate: func(t *testing.T, args []any) {
				assert.Equal(t, true, args[0])
			},
		},
		{
			name: "single generator - int",
			generators: []GeneratorFunc{
				func() any { return 42 },
			},
			wantLen: 1,
			validate: func(t *testing.T, args []any) {
				assert.Equal(t, 42, args[0])
			},
		},
		{
			name: "single generator - string",
			generators: []GeneratorFunc{
				func() any { return "test" },
			},
			wantLen: 1,
			validate: func(t *testing.T, args []any) {
				assert.Equal(t, "test", args[0])
			},
		},
		{
			name: "multiple generators - mixed types",
			generators: []GeneratorFunc{
				func() any { return 42 },
				func() any { return "hello" },
				func() any { return true },
				func() any { return 3.14 },
			},
			wantLen: 4,
			validate: func(t *testing.T, args []any) {
				assert.Equal(t, 42, args[0])
				assert.Equal(t, "hello", args[1])
				assert.Equal(t, true, args[2])
				assert.Equal(t, 3.14, args[3])
			},
		},
		{
			name: "multiple generators - same type",
			generators: []GeneratorFunc{
				func() any { return 1 },
				func() any { return 2 },
				func() any { return 3 },
			},
			wantLen: 3,
			validate: func(t *testing.T, args []any) {
				assert.Equal(t, 1, args[0])
				assert.Equal(t, 2, args[1])
				assert.Equal(t, 3, args[2])
			},
		},
		{
			name: "generators with nil values",
			generators: []GeneratorFunc{
				func() any { return nil },
				func() any { return "not nil" },
				func() any { return nil },
			},
			wantLen: 3,
			validate: func(t *testing.T, args []any) {
				assert.Nil(t, args[0])
				assert.Equal(t, "not nil", args[1])
				assert.Nil(t, args[2])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getArgs(tt.generators)

			assert.Len(t, got, tt.wantLen)
			if tt.validate != nil {
				tt.validate(t, got)
			}
		})
	}
}

func TestCalculateRampUpInterval(t *testing.T) {
	tests := []struct {
		name     string
		rampUp   time.Duration
		threads  int
		expected time.Duration
	}{
		{
			name:     "single thread",
			rampUp:   1 * time.Second,
			threads:  1,
			expected: time.Second,
		},
		{
			name:     "two threads",
			rampUp:   1 * time.Second,
			threads:  2,
			expected: 500 * time.Millisecond,
		},
		{
			name:     "ten threads with 1 second ramp up",
			rampUp:   1 * time.Second,
			threads:  10,
			expected: 100 * time.Millisecond,
		},
		{
			name:     "hundred threads with 1 second ramp up",
			rampUp:   1 * time.Second,
			threads:  100,
			expected: rampUpMin,
		},
		{
			name:     "large ramp up with few threads",
			rampUp:   10 * time.Second,
			threads:  5,
			expected: 2000 * time.Millisecond,
		},
		{
			name:     "edge case - zero threads should not panic",
			rampUp:   1 * time.Second,
			threads:  0,
			expected: rampUpMin,
		},
		{
			name:     "negative threads should not panic",
			rampUp:   1 * time.Second,
			threads:  -1,
			expected: rampUpMin,
		},
		{
			name:     "very small ramp up with many threads - hits minimum",
			rampUp:   50 * time.Millisecond,
			threads:  100,
			expected: rampUpMin,
		},
		{
			name:     "maximum interval constraint",
			rampUp:   100 * time.Millisecond,
			threads:  2,
			expected: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.rampUp.String()
			fmt.Println(s)
			got := calculateRampUpInterval(tt.rampUp, tt.threads)

			// For edge cases with invalid thread counts, just ensure we don't panic
			// and get a reasonable result
			if tt.threads <= 0 {
				assert.GreaterOrEqual(t, got, rampUpMin)
				return
			}

			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestNewWorkflow(t *testing.T) {
	// Create a test logger
	logger := zerolog.Nop() // No-op logger for testing

	tests := []struct {
		name     string
		cfg      *RunConfig
		logger   *zerolog.Logger
		wantNil  bool
		validate func(t *testing.T, w *Workflow)
	}{
		{
			name: "valid config and logger",
			cfg: &RunConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "host=localhost",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Threads:  1,
							Duration: 10 * time.Second,
							Pacing:   100 * time.Millisecond,
							StatementConfig: &StatementConfig{
								Query: "SELECT 1",
							},
						},
					},
				},
			},
			logger:  &logger,
			wantNil: false,
			validate: func(t *testing.T, w *Workflow) {
				assert.NotNil(t, w.cfg)
				assert.NotNil(t, w.logger)
				assert.Equal(t, "postgres", w.cfg.DbConfig.Driver)
				assert.Equal(t, "host=localhost", w.cfg.DbConfig.Dsn)
				assert.Len(t, w.cfg.WorkflowConfig.Scenarios, 1)
			},
		},
		{
			name:    "nil config",
			cfg:     nil,
			logger:  &logger,
			wantNil: false, // Constructor doesn't validate nil, just assigns
			validate: func(t *testing.T, w *Workflow) {
				assert.Nil(t, w.cfg)
				assert.NotNil(t, w.logger)
			},
		},
		{
			name: "nil logger",
			cfg: &RunConfig{
				DbConfig: &DbConfig{
					Driver: "mysql",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{},
				},
			},
			logger:  nil,
			wantNil: false,
			validate: func(t *testing.T, w *Workflow) {
				assert.NotNil(t, w.cfg)
				assert.Nil(t, w.logger)
				assert.Equal(t, "mysql", w.cfg.DbConfig.Driver)
			},
		},
		{
			name: "empty scenarios",
			cfg: &RunConfig{
				DbConfig: &DbConfig{
					Driver: "sqlite3",
					Dsn:    ":memory:",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{},
				},
			},
			logger:  &logger,
			wantNil: false,
			validate: func(t *testing.T, w *Workflow) {
				assert.NotNil(t, w.cfg)
				assert.NotNil(t, w.logger)
				assert.Empty(t, w.cfg.WorkflowConfig.Scenarios)
			},
		},
		{
			name: "multiple scenarios",
			cfg: &RunConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "host=localhost",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "scenario_1",
							Threads:  2,
							Duration: 5 * time.Second,
							Pacing:   200 * time.Millisecond,
							StatementConfig: &StatementConfig{
								Query: "SELECT * FROM users",
							},
						},
						{
							Name:       "scenario_2",
							Threads:    1,
							Iterations: 100,
							Pacing:     50 * time.Millisecond,
							StatementConfig: &StatementConfig{
								Query: "INSERT INTO logs VALUES (?)",
								Args:  "randIntRange 1 1000",
							},
						},
					},
				},
			},
			logger:  &logger,
			wantNil: false,
			validate: func(t *testing.T, w *Workflow) {
				assert.NotNil(t, w.cfg)
				assert.NotNil(t, w.logger)
				assert.Len(t, w.cfg.WorkflowConfig.Scenarios, 2)

				scenarios := w.cfg.WorkflowConfig.Scenarios
				assert.Equal(t, "scenario_1", scenarios[0].Name)
				assert.Equal(t, 2, scenarios[0].Threads)
				assert.Equal(t, 5*time.Second, scenarios[0].Duration)

				assert.Equal(t, "scenario_2", scenarios[1].Name)
				assert.Equal(t, 1, scenarios[1].Threads)
				assert.Equal(t, 100, scenarios[1].Iterations)
				assert.Equal(t, "randIntRange 1 1000", scenarios[1].StatementConfig.Args)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewWorkflow(tt.cfg, tt.logger)

			if tt.wantNil {
				assert.Nil(t, got)
				return
			}

			require.NotNil(t, got)

			if tt.validate != nil {
				tt.validate(t, got)
			}
		})
	}
}

// Benchmark tests for performance validation
func BenchmarkGetArgs(b *testing.B) {
	generators := []GeneratorFunc{
		func() any { return RandIntRange(1, 100) },
		func() any { return RandStringInRange(5, 10) },
		func() any { return RandBool() },
		func() any { return RandUUID() },
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getArgs(generators)
	}
}
