/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import "time"

// Base structure for another flags
// New flag must extend this structures
type BaseFlag struct {
	Name        string
	ShortName   string
	Description string
}

type RunTestFlag struct {
	BaseFlag
	Value        string
	DefaultValue string
}

func NewRunTestFlag() *RunTestFlag {
	return &RunTestFlag{
		BaseFlag: BaseFlag{
			Name:        "run-test",
			ShortName:   "rt",
			Description: "Path to your *.yaml file for running simple test",
		},
		DefaultValue: "",
	}
}

type VersionFlag struct {
	BaseFlag
	Value        bool
	DefaultValue bool
}

func NewVersionFlag() *VersionFlag {
	return &VersionFlag{
		BaseFlag: BaseFlag{
			Name:        "version",
			ShortName:   "v",
			Description: "Get LoadHound version",
		},
		DefaultValue: false,
	}
}

type RunTestConfig struct {
	DbConfig            DbConfig            `yaml:"db"`
	QueryTemplateConfig QueryTemplateConfig `yaml:"query_template"`
	TestConfig          TestConfig          `yaml:"test"`
	OutputConfig        OutputConfig        `yaml:"output"`
}

type DbConfig struct {
	Driver             string        `yaml:"driver" validate:"required"`
	Dsn                string        `yaml:"dsn" validate:"required"`
	MaxOpenConnections int           `yaml:"max_open_connections"`
	MaxIdleConnections int           `yaml:"max_idle_connections"`
	ConnMaxIdleTime    time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifeTime    time.Duration `yaml:"conn_max_life_time"`
	UseStmt            bool          `yaml:"use_prepared_statements"`
}

type QueryTemplateConfig struct {
	PathToQuery string `yaml:"path_to_query"`
	InlineQuery string `yaml:"inline_query"`
}

type TestConfig struct {
	Type       string        `yaml:"type"`
	Iterations int           `yaml:"iterations"`
	Duration   time.Duration `yaml:"duration"`
	Workers    int           `yaml:"workers" validate:"required,min=1"`
	Pacing     time.Duration `yaml:"pacing"`
}

type OutputConfig struct {
	ReportType string    `yaml:"report_type"`
	ReportPath string    `yaml:"report_path"`
	LogConfig  LogConfig `yaml:"log"`
}

type LogConfig struct {
	FileWriter    bool `yaml:"file_writer"`
	ConsoleWriter bool `yaml:"console_writer"`
}
