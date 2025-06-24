package model

import (
	"time"

	"github.com/rs/zerolog"
)

type QuickRun struct {
	Driver        string
	Dsn           string
	PathToSQLFile string
	QueryTemplate string
	Workers       int
	Iterations    int
	Duration      time.Duration
	Pacing        time.Duration
	OutputFile    string
	UseStmt       bool
	Logger        *zerolog.Logger
}
