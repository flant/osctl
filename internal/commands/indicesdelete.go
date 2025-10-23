package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"fmt"

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
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	pattern, _ := cmd.Flags().GetString("pattern")

	if pattern == "" {
		return fmt.Errorf("pattern parameter is required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	_ = client

	logger.Info("Indices delete command not implemented yet")
	return nil
}
