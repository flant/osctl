package commands

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "curator-go",
	Short: "OpenSearch indices lifecycle management tool",
	Long:  `curator-go is a tool for managing OpenSearch clusters indices.`,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(retentionCmd)
	rootCmd.AddCommand(shardingCmd)
	rootCmd.AddCommand(indexPatternsCmd)
	rootCmd.AddCommand(dataSourceCmd)
	rootCmd.AddCommand(dereplicatorCmd)
	rootCmd.AddCommand(snapshotCheckerCmd)
	rootCmd.AddCommand(danglingCheckerCmd)
	rootCmd.AddCommand(coldStorageCmd)
	rootCmd.AddCommand(extractedDeleteCmd)
	rootCmd.AddCommand(snapshotDeleteCmd)
	rootCmd.AddCommand(indicesDeleteCmd)
}

func addCommonFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("os-url", "https://opendistro:9200", "OpenSearch URL")
	cmd.PersistentFlags().String("cert-file", "/etc/ssl/certs/admin-crt.pem", "Certificate file path")
	cmd.PersistentFlags().String("key-file", "/etc/ssl/certs/admin-key.pem", "Key file path")
	cmd.PersistentFlags().String("ca-file", "/etc/ssl/certs/elk-root-ca.pem", "CA file path")
	cmd.PersistentFlags().String("log-level", "info", "Log level (debug, info, warn, error)")
	cmd.PersistentFlags().String("log-format", "json", "Log format (json, text)")
	cmd.PersistentFlags().Duration("timeout", 30, "Request timeout")
	cmd.PersistentFlags().Int("retry-attempts", 3, "Number of retry attempts")
}
