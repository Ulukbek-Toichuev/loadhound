/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

type RunTestConfig struct {
	DbConfig       *DbConfig       `toml:"db" json:"db" validate:"required"`
	WorkflowConfig *WorkflowConfig `toml:"workflow" json:"workflow" validate:"required"`
	OutputConfig   *OutputConfig   `toml:"output" json:"output"`
}

type DbConfig struct {
	Driver       string        `toml:"driver" json:"driver" validate:"required"`
	Dsn          string        `toml:"dsn" json:"dsn" validate:"required"`
	QueryTimeout time.Duration `toml:"query_timeout" json:"query_timeout"`
	ConnPoolCfg  *ConnPoolCfg  `toml:"conn_pool" json:"conn_pool"`
}

type ConnPoolCfg struct {
	MaxOpenConnections int           `toml:"max_open_connections" json:"max_open_connections"`
	MaxIdleConnections int           `toml:"max_idle_connections" json:"max_idle_connections"`
	ConnMaxIdleTime    time.Duration `toml:"conn_max_idle_time" json:"conn_max_idle_time"`
	ConnMaxLifeTime    time.Duration `toml:"conn_max_life_time" json:"conn_max_life_time"`
}

type WorkflowConfig struct {
	Scenarios []*ScenarioConfig `toml:"scenarios" json:"scenarios" validate:"required"`
}

type ScenarioConfig struct {
	Iterations      int              `toml:"iterations" json:"iterations"`
	Duration        time.Duration    `toml:"duration" json:"duration"`
	Threads         int              `toml:"threads" json:"threads" validate:"required,min=1"`
	Pacing          time.Duration    `toml:"pacing" json:"pacing"`
	RampUp          time.Duration    `toml:"ramp_up" json:"ramp_up"`
	StatementConfig *StatementConfig `toml:"statement" json:"statement" validate:"required"`
}

type StatementConfig struct {
	Name  string `toml:"name" json:"name"`
	Query string `toml:"query" json:"query" validate:"required"`
	Args  string `toml:"args" json:"args"`
}

type OutputConfig struct {
	ReportConfig   *ReportConfig   `toml:"report" json:"report"`
	LogConfig      *LogConfig      `toml:"log" json:"log"`
	InfluxDBConfig *InfluxDBConfig `toml:"influx_db" json:"influxdb"`
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

type InfluxDBConfig struct {
	Bucket  string        `toml:"bucket"`
	Org     string        `toml:"org"`
	Token   string        `toml:"token"`
	Url     string        `toml:"url"`
	Period  time.Duration `toml:"period" json:"period"`
	TimeOut time.Duration `toml:"timeout" json:"timeout"`
}

func ReadConfigFile(path string, out *RunTestConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func ValidateConfig(cfg RunTestConfig) error {
	scenarios := cfg.WorkflowConfig.Scenarios
	for _, sc := range scenarios {
		dur := sc.Duration
		iter := sc.Iterations
		pacing := sc.Pacing
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
