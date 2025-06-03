package cmd

import (
	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/spf13/cobra"
)

func GetRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run test scenario",
		RunE: func(cmd *cobra.Command, args []string) error {
			return internal.RunHandler()
		},
	}
	return cmd
}
