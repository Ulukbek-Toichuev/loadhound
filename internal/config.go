/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// RunConfig represents the top-level test configuration.
// It combines database, workflow, and output settings.
type RunConfig struct {
	DbConfig       *DbConfig       `toml:"db" json:"db"`
	WorkflowConfig *WorkflowConfig `toml:"workflow" json:"workflow"`
	OutputConfig   *OutputConfig   `toml:"output" json:"output"`
}

// DbConfig defines settings required to connect to the database.
type DbConfig struct {
	Driver      string       `toml:"driver" json:"driver"`       // e.g., "postgres"
	Dsn         string       `toml:"dsn" json:"dsn"`             // connection string
	ConnPoolCfg *ConnPoolCfg `toml:"conn_pool" json:"conn_pool"` // optional connection pool settings
}

// ConnPoolCfg contains optional connection pool settings for database clients.
type ConnPoolCfg struct {
	MaxOpenConnections int           `toml:"max_open_connections" json:"max_open_connections"`
	MaxIdleConnections int           `toml:"max_idle_connections" json:"max_idle_connections"`
	ConnMaxIdleTime    time.Duration `toml:"conn_max_idle_time" json:"conn_max_idle_time"`
	ConnMaxLifeTime    time.Duration `toml:"conn_max_life_time" json:"conn_max_life_time"`
}

func (cp *ConnPoolCfg) MarshalJSON() ([]byte, error) {
	type AliasCP ConnPoolCfg
	return json.Marshal(&struct {
		ConnMaxIdleTime string `json:"conn_max_idle_time"`
		ConnMaxLifeTime string `json:"conn_max_life_time"`
		*AliasCP
	}{
		ConnMaxIdleTime: cp.ConnMaxIdleTime.String(),
		ConnMaxLifeTime: cp.ConnMaxLifeTime.String(),
		AliasCP:         (*AliasCP)(cp),
	})
}

// WorkflowConfig holds a list of scenario configurations.
// Each scenario defines a unique load testing pattern.
type WorkflowConfig struct {
	Scenarios []*ScenarioConfig `toml:"scenarios" json:"scenarios"`
}

// ScenarioConfig defines one specific load testing scenario.
// Either Duration or Iterations must be set (but not both).
type ScenarioConfig struct {
	Name            string           `toml:"name" json:"name"`             // Scenario name
	Iterations      int              `toml:"iterations" json:"iterations"` // Number of iterations per thread
	Duration        time.Duration    `toml:"duration" json:"duration"`     // Total duration of the scenario
	Threads         int              `toml:"threads" json:"threads"`       // Number of concurrent threads
	Pacing          time.Duration    `toml:"pacing" json:"pacing"`         // Delay between thread iterations
	RampUp          time.Duration    `toml:"ramp_up" json:"ramp_up"`       // Time to ramp from 0 to N threads
	StatementConfig *StatementConfig `toml:"statement" json:"statement"`   // SQL statement to execute
}

func (sc *ScenarioConfig) MarshalJSON() ([]byte, error) {
	type AliasSC ScenarioConfig
	return json.Marshal(&struct {
		Duration string `json:"duration"`
		Pacing   string `json:"pacing"`
		RampUp   string `json:"ramp_up"`
		*AliasSC
	}{
		Duration: sc.Duration.String(),
		Pacing:   sc.Pacing.String(),
		RampUp:   sc.RampUp.String(),
		AliasSC:  (*AliasSC)(sc),
	})
}

// StatementConfig holds the SQL query definition used by each scenario.
type StatementConfig struct {
	Name        string `toml:"name" json:"name"`                   // Optional label
	PathToQuery string `toml:"path_to_query" json:"path_to_query"` // Path to file which contains query
	Query       string `toml:"query" json:"query"`                 // SQL query text
	Args        string `toml:"args" json:"args"`                   // Optional arguments for parameterized queries
}

// OutputConfig specifies how test results are reported and logged.
type OutputConfig struct {
	ReportConfig *ReportConfig `toml:"report" json:"report"`
	LogConfig    *LogConfig    `toml:"log" json:"log"`
}

// ReportConfig defines whether the test report should be printed to console, written to file, or both.
type ReportConfig struct {
	ToFile    bool `toml:"to_file" json:"to_file"`
	ToConsole bool `toml:"to_console" json:"to_console"`
}

// LogConfig defines logging behavior and destination.
type LogConfig struct {
	Level     string `toml:"level" json:"level"`           // "trace", "debug", "info", "error"....
	ToFile    bool   `toml:"to_file" json:"to_file"`       // Write logs to file
	ToConsole bool   `toml:"to_console" json:"to_console"` // Print logs to console
}

func GetConfig(path string) (*RunConfig, error) {
	var cfg RunConfig
	if err := readConfigFile(path, &cfg); err != nil {
		return nil, err
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	// Save query from file into field 'query' in statement config
	for _, sc := range cfg.WorkflowConfig.Scenarios {
		if sc.StatementConfig.PathToQuery != "" {
			data, err := os.ReadFile(sc.StatementConfig.PathToQuery)
			if err != nil {
				return nil, err
			}
			sc.StatementConfig.Query = string(data)
		}
	}
	return &cfg, nil
}

// Read data from path to config
func readConfigFile(path string, out *RunConfig) error {
	cleanPath := filepath.Clean(path)
	if strings.Contains(cleanPath, "..") {
		return errors.New("invalid file path: path traversal detected")
	}
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

// Validate config
func validateConfig(cfg *RunConfig) error {
	if cfg == nil {
		return errors.New("configuration is nil")
	}
	// Validate database configuration
	if cfg.DbConfig == nil {
		return errors.New("database configuration is nil")
	}
	// Validate database driver type
	if cfg.DbConfig.Driver == "" {
		return errors.New("database driver is empty")
	}
	// Validate database destination
	if cfg.DbConfig.Dsn == "" {
		return errors.New("database destination (dsn) is empty")
	}

	// Validate workflow
	if cfg.WorkflowConfig == nil {
		return errors.New("workflow is nil")
	}

	// Validate scenarios configuration list
	if cfg.WorkflowConfig.Scenarios == nil {
		return errors.New("non scenarios set for test")
	}
	if len(cfg.WorkflowConfig.Scenarios) == 0 {
		return errors.New("non scenarios set for test")
	}

	// Range and validate in scenarios configuration list
	for _, sc := range cfg.WorkflowConfig.Scenarios {
		dur := sc.Duration
		iter := sc.Iterations
		pacing := sc.Pacing
		if dur == 0 && iter == 0 {
			return fmt.Errorf("either duration: (%v) or iteration: (%d) must be set", dur, iter)
		}
		if dur > 0 && iter > 0 {
			return fmt.Errorf("duration: (%v) and iteration: (%d) are mutual exclusion - specify only one", dur, iter)
		}
		if dur > 0 && pacing > dur {
			return fmt.Errorf("pacing: (%v) cannot be more than test duration: (%v)", pacing, dur)
		}
		if sc.Threads <= 0 {
			return errors.New("threads count must be >= 1")
		}

		// Validate scenarios statement config
		if sc.StatementConfig == nil {
			return errors.New("statement is nil")
		}

		// Validate statement query source
		if sc.StatementConfig.Query == "" && sc.StatementConfig.PathToQuery == "" {
			return errors.New("query is empty")
		}
		if sc.StatementConfig.Query != "" && sc.StatementConfig.PathToQuery != "" {
			return fmt.Errorf("query: (%s) and path to file with query: (%s) are mutual exclusion - specify only one",
				sc.StatementConfig.Query, sc.StatementConfig.PathToQuery)
		}
	}
	return nil
}
