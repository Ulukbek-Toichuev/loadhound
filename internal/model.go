/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"time"
)

type RunTestConfig struct {
	DbConfig       *DbConfig       `toml:"db" json:"db" validate:"required"`
	WorkflowConfig *WorkflowConfig `toml:"workflow" json:"workflow" validate:"required"`
	OutputConfig   *OutputConfig   `toml:"output" json:"output"`
}

type DbConfig struct {
	Driver    string     `toml:"driver" json:"driver" validate:"required"`
	Dsn       string     `toml:"dsn" json:"dsn" validate:"required"`
	SQLConfig *SQLConfig `toml:"sql_config" json:"sql_config"`
}

type SQLConfig struct {
	MaxOpenConnections int           `toml:"max_open_connections" json:"max_open_connections"`
	MaxIdleConnections int           `toml:"max_idle_connections" json:"max_idle_connections"`
	ConnMaxIdleTime    time.Duration `toml:"conn_max_idle_time" json:"conn_max_idle_time"`
	ConnMaxLifeTime    time.Duration `toml:"conn_max_life_time" json:"conn_max_life_time"`
	UseStmt            bool          `toml:"use_prepared_statements" json:"use_prepared_statements"`
}

type QueryTemplateConfig struct {
	Name     string `toml:"name" json:"name"`
	Template string `toml:"template" json:"template" validate:"required"`
}

type WorkflowConfig struct {
	Type                string               `toml:"type" json:"type"`
	Iterations          int                  `toml:"iterations" json:"iterations"`
	Duration            time.Duration        `toml:"duration" json:"duration"`
	Threads             int                  `toml:"threads" json:"threads" validate:"required,min=1"`
	Pacing              time.Duration        `toml:"pacing" json:"pacing"`
	QueryTemplateConfig *QueryTemplateConfig `toml:"query_template" json:"query_tempalte" validate:"required"`
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
	ToFile    bool `toml:"to_file" json:"to_file"`
	ToConsole bool `toml:"to_console" json:"to_console"`
}
