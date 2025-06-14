/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package executor

import (
	"context"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"

	"github.com/rs/zerolog"
)

type QuickRunError struct {
	Message string
	Err     error
}

func NewQuickRunError(msg string, err error) *QuickRunError {
	return &QuickRunError{msg, err}
}

func (q *QuickRunError) Error() string {
	return q.Message
}

func (q *QuickRunError) Unwrap() error {
	return q.Err
}

type QuickRun struct {
	Dsn        string
	Query      string
	Workers    int
	Iterations int
	Duration   time.Duration
	Pacing     time.Duration
	OutputFile string
	Logger     *zerolog.Logger
}

func NewQuickRun(dsn, query string, workers, iterations int, duration, pacing time.Duration, outputFile string, logger *zerolog.Logger) *QuickRun {
	return &QuickRun{dsn, query, workers, iterations, duration, pacing, outputFile, logger}
}

func QuickRunHandler(ctx context.Context, qr *QuickRun, exec Executor) error {
	startTestTime := time.Now()
	st := exec.Run(ctx)
	if st == nil {
		errMsg := "unexpected situation, stat pointer is null"
		qr.Logger.Error().Msg(errMsg)
		return NewQuickRunError(errMsg, nil)
	}
	result := stat.GetResult(startTestTime, time.Now(), st)
	qr.Logger.Info().Msg("finished the test!")
	stat.PrintResultPretty(result)
	if qr.OutputFile != "" {
		stat.SaveInFile(result, qr.OutputFile)
	}
	return nil
}

func ValidateQuickRunFields(qr *QuickRun) error {
	if qr.Dsn == "" {
		return NewQuickRunError("--dsn is required", nil)
	}

	if qr.Query == "" {
		return NewQuickRunError("--query is required", nil)
	}

	if qr.Workers < 0 {
		return NewQuickRunError("--workers must be >= 0", nil)
	}

	iterations := qr.Iterations
	duration := qr.Duration

	if iterations < 0 {
		return NewQuickRunError("--iterations must be >= 0", nil)
	}

	if duration < 0 {
		return NewQuickRunError("--duration must be >= 0", nil)
	}

	if iterations == 0 && duration == 0 {
		return NewQuickRunError("either --iter or --duration must be set", nil)
	}

	if iterations > 0 && duration > 0 {
		return NewQuickRunError("--iter and --duration are mutually exclusive", nil)
	}

	if qr.Pacing < 0 {
		return NewQuickRunError("--pacing must be > 0", nil)
	}
	return nil
}
