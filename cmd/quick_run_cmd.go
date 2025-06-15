/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package cmd

import (
	"context"
	"os"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/executor"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/spf13/cobra"
)

const (
	default_workers     int           = 1
	default_iteration   int           = 0
	default_duration    time.Duration = 0
	default_pacing      time.Duration = 0
	default_output_file string        = ""
)

func GetQuickRunCmd() *cobra.Command {
	var qr executor.QuickRun

	cmd := &cobra.Command{
		Use:   "quick-run",
		Short: "Run a quick one-off load test without YAML config",
		RunE: func(cmd *cobra.Command, args []string) error {
			return GetQuickRunHandler(cmd.Context(), &qr)
		},
	}

	cmd.Flags().StringVar(&qr.Dsn, "dsn", "", "PostgreSQL DSN connection string (required)")
	cmd.Flags().StringVar(&qr.PathToSQLFile, "sql-file", "", "Path to your sql file")
	cmd.Flags().StringVar(&qr.Query, "query", "", "SQL query to run (required)")
	cmd.Flags().IntVar(&qr.Workers, "workers", default_workers, "Number of concurrent workers")
	cmd.Flags().IntVar(&qr.Iterations, "iter", default_iteration, "Number of iterations (mutually exclusive with --duration)")
	cmd.Flags().DurationVar(&qr.Duration, "duration", default_duration, "Duration of the test (e.g., 10s, 1m)")
	cmd.Flags().DurationVar(&qr.Pacing, "pacing", default_pacing, "Time between iterations per worker")
	cmd.Flags().StringVar(&qr.OutputFile, "output-file", default_output_file, "File to write test results to")

	return cmd
}

func GetQuickRunHandler(ctx context.Context, qr *executor.QuickRun) error {
	qr.Logger = pkg.GetLogger()
	qr.Logger.Info().Msg("validating quick-run parameters")
	err := executor.ValidateQuickRunFields(qr)
	if err != nil {
		errMsg := "failed to validate parameters"
		qr.Logger.Err(err).Msg(errMsg)
		return executor.NewQuickRunError(errMsg, err)
	}

	if qr.PathToSQLFile != "" {
		query, err := getSQLFromFile(qr.PathToSQLFile)
		if err != nil {
			return err
		}

		qr.Query = query
	}
	qr.Logger.Info().Msg("parsing query template")
	tmpl, err := parse.ParseQueryTemplate(qr.Query)
	if err != nil {
		errMsg := "failed to get template"
		qr.Logger.Err(err).Msg(errMsg)
		return executor.NewQuickRunError(errMsg, err)
	}

	qr.Logger.Info().Msg("getting executor instance")
	le, err := executor.NewQuickExecutor(ctx, qr, tmpl)
	if err != nil {
		errMsg := "failed to create executor instance"
		qr.Logger.Err(err).Msg(errMsg)
		return executor.NewQuickRunError(errMsg, err)
	}

	return executor.QuickRunHandler(ctx, qr, le)
}

func getSQLFromFile(pathToFile string) (string, error) {
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
