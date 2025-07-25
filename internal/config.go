/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-playground/validator/v10"
)

// RunConfig represents the top-level test configuration.
// It combines database, workflow, and output settings.
type RunConfig struct {
	DbConfig       *DbConfig       `toml:"db" json:"db" validate:"required"`
	WorkflowConfig *WorkflowConfig `toml:"workflow" json:"workflow" validate:"required"`
	OutputConfig   *OutputConfig   `toml:"output" json:"output"`
}

// DbConfig defines settings required to connect to the database.
type DbConfig struct {
	Driver      string       `toml:"driver" json:"driver" validate:"required"` // e.g., "postgres"
	Dsn         string       `toml:"dsn" json:"dsn" validate:"required"`       // connection string
	ConnPoolCfg *ConnPoolCfg `toml:"conn_pool" json:"conn_pool"`               // optional connection pool settings
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
	Scenarios []*ScenarioConfig `toml:"scenarios" json:"scenarios" validate:"required"`
}

// ScenarioConfig defines one specific load testing scenario.
// Either Duration or Iterations must be set (but not both).
type ScenarioConfig struct {
	Name            string           `toml:"name" json:"name"`                                 // Scenario name
	Iterations      int              `toml:"iterations" json:"iterations"`                     // Number of iterations per thread
	Duration        time.Duration    `toml:"duration" json:"duration"`                         // Total duration of the scenario
	Threads         int              `toml:"threads" json:"threads" validate:"required,min=1"` // Number of concurrent threads
	Pacing          time.Duration    `toml:"pacing" json:"pacing"`                             // Delay between thread iterations
	RampUp          time.Duration    `toml:"ramp_up" json:"ramp_up"`                           // Time to ramp from 0 to N threads
	StatementConfig *StatementConfig `toml:"statement" json:"statement" validate:"required"`   // SQL statement to execute
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
	Name  string `toml:"name" json:"name"`                       // Optional label
	Query string `toml:"query" json:"query" validate:"required"` // SQL query text
	Args  string `toml:"args" json:"args"`                       // Optional arguments for parameterized queries
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
	return &cfg, nil
}

func readConfigFile(path string, out *RunConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func validateConfig(cfg *RunConfig) error {
	validate := validator.New(validator.WithRequiredStructEnabled())
	if err := validate.Struct(cfg); err != nil {
		return err
	}

	if cfg.WorkflowConfig.Scenarios == nil {
		return errors.New("non scenarios set for test")
	}

	if len(cfg.WorkflowConfig.Scenarios) == 0 {
		return errors.New("non scenarios set for test")
	}

	for _, sc := range cfg.WorkflowConfig.Scenarios {
		dur := sc.Duration
		iter := sc.Iterations
		pacing := sc.Pacing
		threads := sc.Threads
		if sc.StatementConfig == nil {
			return errors.New("statement cannot be nil")
		}
		if sc.StatementConfig.Query == "" {
			return errors.New("query cannot be empty")
		}
		if threads < 1 {
			return errors.New("threads count must be >= 1")
		}
		if dur == 0 && iter == 0 {
			return fmt.Errorf("either duration: %v or iteration: %d must be set", dur, iter)
		}
		if dur > 0 && iter > 0 {
			return fmt.Errorf("duration: %v and iteration: %d are mutual exclusion", dur, iter)
		}
		if dur > 0 && pacing > dur {
			return fmt.Errorf("pacing: %v cannot be more than test duration: %v", pacing, dur)
		}
	}

	return nil
}
