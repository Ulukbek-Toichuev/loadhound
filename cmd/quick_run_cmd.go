/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package cmd

import (
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/spf13/cobra"
)

const (
	default_workers     int           = 1
	default_iteration   int           = 1
	default_duration    time.Duration = 1
	default_pacing      time.Duration = 1
	default_output_file string        = "result.json"
)

func GetQuickRunCmd() *cobra.Command {
	var qr internal.QuickRun

	cmd := &cobra.Command{
		Use:   "quick-run",
		Short: "Run a quick one-off load test without YAML config",
		RunE: func(cmd *cobra.Command, args []string) error {
			return internal.QuickRunHandler(cmd.Context(), &qr)
		},
	}

	cmd.Flags().StringVar(&qr.Dsn, "dsn", "", "PostgreSQL DSN connection string (required)")
	cmd.Flags().StringVar(&qr.Query, "query", "", "SQL query to run (required)")
	cmd.Flags().IntVar(&qr.Workers, "workers", default_workers, "Number of concurrent workers")
	cmd.Flags().IntVar(&qr.Iterations, "iter", default_iteration, "Number of iterations (mutually exclusive with --duration)")
	cmd.Flags().DurationVar(&qr.Duration, "duration", default_duration, "Duration of the test (e.g., 10s, 1m)")
	cmd.Flags().DurationVar(&qr.Pacing, "pacing", default_pacing, "Time between iterations per worker")
	cmd.Flags().StringVar(&qr.OutputFile, "output-file", default_output_file, "File to write test results to")

	return cmd
}
