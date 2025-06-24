/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/executor"
	"github.com/Ulukbek-Toichuev/loadhound/internal/model"
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
	defaultStmtMode   bool          = false
)

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
	cmd.Flags().StringVar(&cfg.QueryTemplate, "query", "", "SQL query to run (required)")
	cmd.Flags().IntVar(&cfg.Workers, "workers", defaultWorkers, "Number of concurrent workers")
	cmd.Flags().IntVar(&cfg.Iterations, "iter", defaultIteration, "Number of iterations (mutually exclusive with --duration)")
	cmd.Flags().DurationVar(&cfg.Duration, "duration", defaultDuration, "Duration of the test (e.g., 10s, 1m)")
	cmd.Flags().DurationVar(&cfg.Pacing, "pacing", defaultPacing, "Time between iterations per worker")
	cmd.Flags().StringVar(&cfg.OutputFile, "output-file", defaultOutputFile, "File to write test results to")
	cmd.Flags().BoolVar(&cfg.UseStmt, "use-stmt", defaultStmtMode, "Using prepared statement")
	return cmd
}

func GetQuickRunHandler(ctx context.Context, qr *model.QuickRun) error {
	qr.Logger.Info().Msg("validating quick-run parameters")
	if err := validateQuickRunFields(qr); err != nil {
		return fmt.Errorf("failed to validate parameters: %v", err)
	}

	if qr.PathToSQLFile != "" {
		queryTmpl, err := getSQLFromFile(qr.PathToSQLFile)
		if err != nil {
			return err
		}

		qr.Logger.Info().Msg("getting query from file")
		qr.QueryTemplate = queryTmpl
	}

	qr.Logger.Info().Msg("getting executor instance")
	executor, err := executor.BuildQuickExecutor(ctx, qr)
	if err != nil {
		return fmt.Errorf("failed to create executor instance: %v", err)
	}

	return quickRunHandler(ctx, qr, executor)
}

func quickRunHandler(ctx context.Context, qr *model.QuickRun, exec Executor) error {
	startTestTime := time.Now()
	time.Sleep(2 * time.Second)
	resultStats := exec.Run(ctx)
	if resultStats == nil {
		return errors.New("unexpected situation, stat pointer is null")
	}
	result := stat.GetResult(startTestTime, time.Now(), resultStats)

	fmt.Printf("\n")
	qr.Logger.Info().Msg("finished the test!")
	stat.PrintResultPretty(result)
	if qr.OutputFile != "" {
		stat.SaveInFile(result, qr.OutputFile)
	}
	return nil
}

func validateQuickRunFields(qr *model.QuickRun) error {
	if qr.Driver == "" {
		return errors.New("driver is required")
	}

	if d := pkg.GetDriverType(qr.Driver); d == pkg.Unknown {
		return errors.New("unsupported driver")
	}

	if qr.Dsn == "" {
		return errors.New("dsn is required")
	}

	if qr.Workers < 0 {
		return errors.New("workers must be positive")
	}

	iterations := qr.Iterations
	duration := qr.Duration

	if iterations < 0 {
		return errors.New("iterations value must be positive")
	}

	if duration < 0 {
		return errors.New("duration must be positive")
	}

	if iterations == 0 && duration == 0 {
		return errors.New("either iterations or duration must be set")
	}

	if iterations > 0 && duration > 0 {
		return errors.New("iterations and duration are mutually exclusive")
	}

	if qr.QueryTemplate == "" && qr.PathToSQLFile == "" {
		return errors.New("either query or sql-file must be set")
	}

	if qr.QueryTemplate != "" && qr.PathToSQLFile != "" {
		return errors.New("query and sql-file are mutually exclusive")
	}

	if qr.Pacing < 0 {
		return errors.New("pacing must be positive")
	}
	return nil
}

func getSQLFromFile(pathToFile string) (string, error) {
	if !strings.HasSuffix(pathToFile, ".sql") {
		return "", errors.New("file is not a .sql file")
	}
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
