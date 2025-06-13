package cmd

import (
	"github.com/Ulukbek-Toichuev/loadhound/internal/executor"

	"github.com/spf13/cobra"
)

func GetRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run test scenario",
		RunE: func(cmd *cobra.Command, args []string) error {
			return executor.RunHandler()
		},
	}
	return cmd
}
