package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"fmt"

	"github.com/spf13/cobra"
)

var extractedDeleteCmd = &cobra.Command{
	Use:   "extracteddelete",
	Short: "Delete extracted indices",
	Long:  `Delete extracted indices that are no longer needed.`,
	RunE:  runExtractedDelete,
}

func init() {
	extractedDeleteCmd.Flags().Int("days", 7, "Number of days to keep extracted indices")
	extractedDeleteCmd.Flags().String("snap-repo", "", "Snapshot repository name")
	extractedDeleteCmd.Flags().Bool("dry-run", false, "Show what would be deleted without actually deleting")

	addCommonFlags(extractedDeleteCmd)
}

func runExtractedDelete(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	snapRepo, _ := cmd.Flags().GetString("snap-repo")

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	_ = client

	logger.Info("Extracted delete command not implemented yet")
	return nil
}
