package cmd

import (
	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/spf13/cobra"
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
	cmd.Flags().IntVar(&qr.Workers, "workers", 1, "Number of concurrent workers")
	cmd.Flags().IntVar(&qr.Iterations, "iter", 0, "Number of iterations (mutually exclusive with --duration)")
	cmd.Flags().DurationVar(&qr.Duration, "duration", 0, "Duration of the test (e.g., 10s, 1m)")
	cmd.Flags().DurationVar(&qr.Pacing, "pacing", 0, "Time between iterations per worker")
	cmd.Flags().StringVar(&qr.OutputFile, "output-file", "results.json", "File to write test results to")

	return cmd
}
