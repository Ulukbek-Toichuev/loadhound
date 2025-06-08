/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
)

type QuickRunErr struct {
	Message string
	Err     error
}

func NewQuickRunErr(msg string, err error) *QuickRunErr {
	return &QuickRunErr{msg, err}
}

func (q *QuickRunErr) Error() string {
	return q.Message
}

func (q *QuickRunErr) Unwrap() error {
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
}

func NewQuickRun(dsn, query string, workers, iterations int, duration, pacing time.Duration, outputFile string) *QuickRun {
	return &QuickRun{dsn, query, workers, iterations, duration, pacing, outputFile}
}

func QuickRunHandler(ctx context.Context, qr *QuickRun) error {
	pkg.LogWrapper("Validating quick test parameters...")
	err := validateQuickRunFields(qr)
	if err != nil {
		return NewQuickRunErr(fmt.Sprintf("failed to validate fields: %s", err.Error()), err)
	}

	pkg.LogWrapper("Parsing query template...")
	tmpl, err := ParseQueryTemplate(qr.Query)
	if err != nil {
		return NewQuickRunErr("failed to get template", err)
	}

	fmt.Println("pacing: ", qr.Pacing)

	startTestTime := time.Now()
	st := ExecuteQuickTest(ctx, qr, tmpl)
	if st != nil {
		summary := stat.NewSummaryStat(st, startTestTime, time.Now(), qr.Workers, qr.Iterations)
		stat.PrintSummary(summary)
		if qr.OutputFile != "" {
			stat.SaveInFile(summary, qr.OutputFile)
		}
	}
	return nil
}

func validateQuickRunFields(qr *QuickRun) error {
	if qr.Dsn == "" {
		return NewParseError("--dsn is required", nil)
	}

	if qr.Query == "" {
		return NewParseError("--query is required", nil)
	}

	if qr.Workers < 0 {
		return NewParseError("--workers must be >= 0", nil)
	}

	iterations := qr.Iterations
	duration := qr.Duration

	if iterations < 0 {
		return NewParseError("--iterations must be >= 0", nil)
	}

	if duration < 0 {
		return NewParseError("--duration must be >= 0", nil)
	}

	if iterations == 0 && duration == 0 {
		return NewParseError("either --iter or --duration must be set", nil)
	}

	if iterations > 0 && duration > 0 {
		return NewParseError("--iter and --duration are mutually exclusive", nil)
	}

	if qr.Pacing < 0 {
		return NewParseError("--pacing must be > 0", nil)
	}
	return nil
}
