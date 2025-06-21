package model

import (
	"time"

	"github.com/rs/zerolog"
)

type QuickRun struct {
	Driver        string
	Dsn           string
	PathToSQLFile string
	Query         string
	Workers       int
	Iterations    int
	Duration      time.Duration
	Pacing        time.Duration
	OutputFile    string
	UsePrepare    bool
	Logger        *zerolog.Logger
}
