/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *RunTestConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config with duration",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
							Pacing:   1 * time.Second,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid config with iterations",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "mysql",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:       "test_scenario",
							Iterations: 100,
							Threads:    3,
							StatementConfig: &StatementConfig{
								Name:  "insert_test",
								Query: "INSERT INTO users (name) VALUES (?)",
								Args:  "John Doe",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid config with pacing equal to duration",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 10 * time.Second,
							Threads:  2,
							Pacing:   10 * time.Second,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT COUNT(*) FROM products",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "missing required db config",
			config: &RunTestConfig{
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "DbConfig",
		},
		{
			name: "missing required workflow config",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
			},
			expectError: true,
			errorMsg:    "WorkflowConfig",
		},
		{
			name: "missing required driver in db config",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Dsn: "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "Driver",
		},
		{
			name: "missing required dsn in db config",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "Dsn",
		},
		{
			name: "missing required scenarios",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{},
				},
			},
			expectError: true,
			errorMsg:    "non scenarios set for test",
		},
		{
			name: "missing required threads in scenario",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  0, // Invalid: threads must be >= 1
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "threads count must be >= 1",
		},
		{
			name: "missing required statement config",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "statement cannot be nil",
		},
		{
			name: "missing required query in statement config",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
							StatementConfig: &StatementConfig{
								Name: "select_test",
								// Query is missing
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "query cannot be empty",
		},
		{
			name: "neither duration nor iterations set",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:    "test_scenario",
							Threads: 5,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "either duration: 0s or iteration: 0 must be set",
		},
		{
			name: "both duration and iterations set",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:       "test_scenario",
							Duration:   30 * time.Second,
							Iterations: 100,
							Threads:    5,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "duration: 30s and iteration: 100 are mutual exclusion",
		},
		{
			name: "pacing greater than duration",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "test_scenario",
							Duration: 10 * time.Second,
							Threads:  5,
							Pacing:   15 * time.Second, // Pacing > Duration
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "pacing: 15s cannot be more than test duration: 10s",
		},
		{
			name: "multiple scenarios with mixed valid/invalid",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "valid_scenario",
							Duration: 30 * time.Second,
							Threads:  5,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
						{
							Name:    "invalid_scenario",
							Threads: 3,
							// Neither duration nor iterations set
							StatementConfig: &StatementConfig{
								Name:  "insert_test",
								Query: "INSERT INTO users (name) VALUES (?)",
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "either duration: 0s or iteration: 0 must be set",
		},
		{
			name: "multiple valid scenarios",
			config: &RunTestConfig{
				DbConfig: &DbConfig{
					Driver: "postgres",
					Dsn:    "user:pass@localhost/db",
				},
				WorkflowConfig: &WorkflowConfig{
					Scenarios: []*ScenarioConfig{
						{
							Name:     "scenario_1",
							Duration: 30 * time.Second,
							Threads:  5,
							Pacing:   2 * time.Second,
							StatementConfig: &StatementConfig{
								Name:  "select_test",
								Query: "SELECT * FROM users",
							},
						},
						{
							Name:       "scenario_2",
							Iterations: 100,
							Threads:    3,
							StatementConfig: &StatementConfig{
								Name:  "insert_test",
								Query: "INSERT INTO users (name) VALUES (?)",
								Args:  "Test User",
							},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.config)

			if tt.expectError {
				require.Error(t, err, "Expected an error but got none")
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, "Expected no error but got: %v", err)
			}
		})
	}
}

func TestValidateConfig_EdgeCases(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		err := ValidateConfig(nil)
		require.Error(t, err)
	})

	t.Run("config with optional fields", func(t *testing.T) {
		config := &RunTestConfig{
			DbConfig: &DbConfig{
				Driver: "postgres",
				Dsn:    "user:pass@localhost/db",
				ConnPoolCfg: &ConnPoolCfg{
					MaxOpenConnections: 10,
					MaxIdleConnections: 5,
					ConnMaxIdleTime:    5 * time.Minute,
					ConnMaxLifeTime:    1 * time.Hour,
				},
			},
			WorkflowConfig: &WorkflowConfig{
				Scenarios: []*ScenarioConfig{
					{
						Name:     "test_scenario",
						Duration: 30 * time.Second,
						Threads:  5,
						RampUp:   5 * time.Second,
						StatementConfig: &StatementConfig{
							Name:  "select_test",
							Query: "SELECT * FROM users WHERE id = ?",
							Args:  "123",
						},
					},
				},
			},
			OutputConfig: &OutputConfig{
				ReportConfig: &ReportConfig{
					ToFile:    true,
					ToConsole: true,
				},
				LogConfig: &LogConfig{
					Level:     "info",
					ToFile:    true,
					ToConsole: false,
				},
			},
		}

		err := ValidateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("zero duration with pacing should not error", func(t *testing.T) {
		config := &RunTestConfig{
			DbConfig: &DbConfig{
				Driver: "postgres",
				Dsn:    "user:pass@localhost/db",
			},
			WorkflowConfig: &WorkflowConfig{
				Scenarios: []*ScenarioConfig{
					{
						Name:       "test_scenario",
						Iterations: 100,
						Threads:    5,
						Pacing:     1 * time.Second, // This should be fine with iterations
						StatementConfig: &StatementConfig{
							Name:  "select_test",
							Query: "SELECT * FROM users",
						},
					},
				},
			},
		}

		err := ValidateConfig(config)
		assert.NoError(t, err)
	})
}
