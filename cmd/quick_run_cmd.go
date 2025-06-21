/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/executor"
	"github.com/Ulukbek-Toichuev/loadhound/internal/model"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/spf13/cobra"
)

const (
	defaultWorkers    int           = 1
	defaultIteration  int           = 0
	defaultDuration   time.Duration = 0
	defaultPacing     time.Duration = 0
	defaultOutputFile string        = ""
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

func GetQuickRunCmd() *cobra.Command {
	var cfg model.QuickRun
	cfg.Logger = pkg.GetLogger()

	cmd := &cobra.Command{
		Use:   "quick-run",
		Short: "Run a quick one-off load test without YAML config",
		RunE: func(cmd *cobra.Command, args []string) error {
			return GetQuickRunHandler(cmd.Context(), &cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.Driver, "driver", "", "Database driver")
	cmd.Flags().StringVar(&cfg.Dsn, "dsn", "", "PostgreSQL DSN connection string (required)")
	cmd.Flags().StringVar(&cfg.PathToSQLFile, "sql-file", "", "Path to your sql file")
	cmd.Flags().StringVar(&cfg.Query, "query", "", "SQL query to run (required)")
	cmd.Flags().IntVar(&cfg.Workers, "workers", defaultWorkers, "Number of concurrent workers")
	cmd.Flags().IntVar(&cfg.Iterations, "iter", defaultIteration, "Number of iterations (mutually exclusive with --duration)")
	cmd.Flags().DurationVar(&cfg.Duration, "duration", defaultDuration, "Duration of the test (e.g., 10s, 1m)")
	cmd.Flags().DurationVar(&cfg.Pacing, "pacing", defaultPacing, "Time between iterations per worker")
	cmd.Flags().StringVar(&cfg.OutputFile, "output-file", defaultOutputFile, "File to write test results to")
	cmd.Flags().BoolVar(&cfg.UsePrepare, "use-prepare", false, "Using prepared statement")
	return cmd
}

func GetQuickRunHandler(ctx context.Context, qr *model.QuickRun) error {
	qr.Logger.Info().Msg("validating quick-run parameters")
	if err := validateQuickRunFields(qr); err != nil {
		errMsg := "failed to validate parameters"
		qr.Logger.Err(err).Msg(errMsg)
		return NewQuickRunError(errMsg, err)
	}

	if qr.PathToSQLFile != "" {
		query, err := getSQLFromFile(qr.PathToSQLFile)
		if err != nil {
			return err
		}

		qr.Logger.Info().Msg("get query from file")
		qr.Query = query
	}

	qr.Logger.Info().Msg("parsing query template")
	tmpl, err := parse.ParseQueryTemplate(qr.Query)
	if err != nil {
		errMsg := "failed to get template"
		qr.Logger.Err(err).Msg(errMsg)
		return NewQuickRunError(errMsg, err)
	}

	qr.Logger.Info().Msg("getting executor instance")
	le, err := executor.BuildQuickExecutor(ctx, qr, tmpl)
	if err != nil {
		errMsg := "failed to create executor instance"
		qr.Logger.Err(err).Msg(errMsg)
		return NewQuickRunError(errMsg, err)
	}

	return quickRunHandler(ctx, qr, le)
}

func quickRunHandler(ctx context.Context, qr *model.QuickRun, exec Executor) error {
	startTestTime := time.Now()
	st := exec.Run(ctx)
	if st == nil {
		errMsg := "unexpected situation, stat pointer is null"
		qr.Logger.Error().Msg(errMsg)
		return NewQuickRunError(errMsg, nil)
	}
	result := stat.GetResult(startTestTime, time.Now(), st)

	fmt.Printf("\n")
	qr.Logger.Info().Msg("finished the test!")
	stat.PrintResultPretty(result)
	if qr.OutputFile != "" {
		stat.SaveInFile(result, qr.OutputFile)
	}
	return nil
}

func getSQLFromFile(pathToFile string) (string, error) {
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func validateQuickRunFields(qr *model.QuickRun) error {
	if qr.Driver == "" {
		return NewQuickRunError("--driver is required", nil)
	}

	if ok := getDriverType(qr.Driver); !ok {
		return NewQuickRunError("unsupported driver", nil)
	}

	if qr.Dsn == "" {
		return NewQuickRunError("--dsn is required", nil)
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

	if qr.Query != "" && qr.PathToSQLFile != "" {
		return NewQuickRunError("--query and --sql-file are mutually exclusive", nil)
	}

	if qr.Query == "" && qr.PathToSQLFile == "" {
		return NewQuickRunError("either --query or --sql-file must be set", nil)
	}

	if !strings.HasSuffix(qr.PathToSQLFile, ".sql") {
		return NewQuickRunError("file is not a .sql file", nil)
	}

	if qr.Pacing < 0 {
		return NewQuickRunError("--pacing must be > 0", nil)
	}
	return nil
}

func getDriverType(d string) bool {
	for _, v := range pkg.AvailableDrivers {
		if v == d {
			return true
		}
	}
	return false
}
