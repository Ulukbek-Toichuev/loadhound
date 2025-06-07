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
	err := ValidateQuickRunFields(qr)
	if err != nil {
		return NewQuickRunErr(fmt.Sprintf("failed to validate fields: %s", err.Error()), err)
	}

	pkg.LogWrapper("Parsing query template...")
	tmpl, err := GetTemplate(qr.Query)
	if err != nil {
		return NewQuickRunErr("failed to get template", err)
	}

	startTestTime := time.Now()
	st := ExecuteQuickTest(ctx, qr, tmpl)
	if st != nil {
		stat.PrintSummary(stat.NewSummaryStat(st, startTestTime, time.Now(), qr.Workers, qr.Iterations))
	}
	return nil
}
