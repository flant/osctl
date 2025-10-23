package commands

import (
	"github.com/spf13/cobra"
)

var indicesDeleteCmd = &cobra.Command{
	Use:   "indicesdelete",
	Short: "Delete indices",
	Long:  `Delete indices`,
	RunE:  runIndicesDelete,
}

func init() {
	indicesDeleteCmd.Flags().String("pattern", "", "Index name pattern to match")
	indicesDeleteCmd.Flags().Int("days", 30, "Number of days to keep indices")

	addCommonFlags(indicesDeleteCmd)
}

func runIndicesDelete(cmd *cobra.Command, args []string) error {
	return nil
}
