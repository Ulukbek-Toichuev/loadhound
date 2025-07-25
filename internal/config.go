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

type RunTestConfig struct {
	DbConfig       *DbConfig       `toml:"db" json:"db" validate:"required"`
	WorkflowConfig *WorkflowConfig `toml:"workflow" json:"workflow" validate:"required"`
	OutputConfig   *OutputConfig   `toml:"output" json:"output"`
}

type DbConfig struct {
	Driver      string       `toml:"driver" json:"driver" validate:"required"`
	Dsn         string       `toml:"dsn" json:"dsn" validate:"required"`
	ConnPoolCfg *ConnPoolCfg `toml:"conn_pool" json:"conn_pool"`
}

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

type WorkflowConfig struct {
	Scenarios []*ScenarioConfig `toml:"scenarios" json:"scenarios" validate:"required"`
}

type ScenarioConfig struct {
	Name            string           `toml:"name" json:"name"`
	Iterations      int              `toml:"iterations" json:"iterations"`
	Duration        time.Duration    `toml:"duration" json:"duration"`
	Threads         int              `toml:"threads" json:"threads" validate:"required,min=1"`
	Pacing          time.Duration    `toml:"pacing" json:"pacing"`
	RampUp          time.Duration    `toml:"ramp_up" json:"ramp_up"`
	StatementConfig *StatementConfig `toml:"statement" json:"statement" validate:"required"`
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

type StatementConfig struct {
	Name  string `toml:"name" json:"name"`
	Query string `toml:"query" json:"query" validate:"required"`
	Args  string `toml:"args" json:"args"`
}

type OutputConfig struct {
	ReportConfig *ReportConfig `toml:"report" json:"report"`
	LogConfig    *LogConfig    `toml:"log" json:"log"`
}

type ReportConfig struct {
	ToFile    bool `toml:"to_file" json:"to_file"`
	ToConsole bool `toml:"to_console" json:"to_console"`
}

type LogConfig struct {
	Level     string `toml:"level" json:"level"`
	ToFile    bool   `toml:"to_file" json:"to_file"`
	ToConsole bool   `toml:"to_console" json:"to_console"`
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
