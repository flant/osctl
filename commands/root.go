package commands

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"

	"github.com/spf13/cobra"
)

var (
	appVersion = ""
)

var rootCmd = &cobra.Command{
	Use:   "osctl",
	Short: "OpenSearch indices lifecycle management tool",
	Long:  `osctl is a tool for managing OpenSearch cluster indices lifecycle.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		actionFlag, _ := cmd.Flags().GetString("action")
		if actionFlag != "" {
			if err := config.ValidateAction(actionFlag); err != nil {
				return err
			}

			if err := config.LoadConfig(cmd, actionFlag); err != nil {
				return err
			}
			return executeActionCommand(actionFlag, args)
		}

		if err := config.LoadConfig(cmd, "root"); err != nil {
			return err
		}
		cfg := config.GetConfig()
		if cfg.Action != "" {
			if err := config.ValidateAction(cfg.Action); err != nil {
				return err
			}

			if err := config.LoadConfig(cmd, cfg.Action); err != nil {
				return err
			}
			return executeActionCommand(cfg.Action, args)
		}

		return cmd.Help()
	},
}

func Execute(version string) error {
	appVersion = version
	if appVersion == "" {
		appVersion = "dev"
	}
	rootCmd.Version = appVersion

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("osctl version=%s", appVersion))

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		commandName := cmd.Name()
		if commandName == "osctl" {
			if action, _ := cmd.Flags().GetString("action"); action != "" {
				return nil
			}
			commandName = "root"
		}

		if err := config.LoadConfig(cmd, commandName); err != nil {
			return err
		}

		return nil
	}

	return rootCmd.Execute()
}

func executeActionCommand(action string, args []string) error {

	var targetCmd *cobra.Command

	switch action {
	case "snapshots":
		targetCmd = snapshotCmd
	case "snapshot-manual":
		targetCmd = snapshotManualCmd
	case "snapshotsdelete":
		targetCmd = snapshotsDeleteCmd
	case "snapshotschecker":
		targetCmd = snapshotsCheckerCmd
	case "indicesdelete":
		targetCmd = indicesDeleteCmd
	case "retention":
		targetCmd = retentionCmd
	case "dereplicator":
		targetCmd = dereplicatorCmd
	case "coldstorage":
		targetCmd = coldStorageCmd
	case "extracteddelete":
		targetCmd = extractedDeleteCmd
	case "danglingchecker":
		targetCmd = danglingCheckerCmd
	case "sharding":
		targetCmd = shardingCmd
	case "indexpatterns":
		targetCmd = indexPatternsCmd
	case "datasource":
		targetCmd = dataSourceCmd
	default:
		return fmt.Errorf("unknown action: %s", action)
	}

	targetCmd.SetArgs(args)
	return targetCmd.RunE(targetCmd, args)
}

func init() {
	addFlags(rootCmd)
	rootCmd.SilenceUsage = true
	rootCmd.Flags().BoolP("version", "v", false, "Print version information")
	rootCmd.SetVersionTemplate("{{.Version}}\n")
	commands := []*cobra.Command{
		snapshotCmd,
		snapshotManualCmd,
		snapshotsDeleteCmd,
		indicesDeleteCmd,
		retentionCmd,
		shardingCmd,
		indexPatternsCmd,
		dataSourceCmd,
		dereplicatorCmd,
		snapshotsCheckerCmd,
		danglingCheckerCmd,
		coldStorageCmd,
		extractedDeleteCmd,
	}
	for _, cmd := range commands {
		cmd.SilenceUsage = true
		rootCmd.AddCommand(cmd)
	}
}

func addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("action", "", "Action to execute (snapshot, retention, dereplicator, etc.)")
	cmd.PersistentFlags().String("config", "", "Path to configuration file")
	cmd.PersistentFlags().String("os-url", "", "OpenSearch URL")
	cmd.PersistentFlags().String("cert-file", "", "Certificate file path")
	cmd.PersistentFlags().String("key-file", "", "Key file path")
	cmd.PersistentFlags().String("ca-file", "", "CA file path")
	cmd.PersistentFlags().Duration("timeout", 0, "Request timeout")
	cmd.PersistentFlags().Int("retry-attempts", 0, "Number of retry attempts")
	cmd.PersistentFlags().String("date-format", "", "Date format for index names")
	cmd.PersistentFlags().String("madison-url", "", "Madison API URL")
	cmd.PersistentFlags().String("osd-url", "", "OpenSearch Dashboards URL")
	cmd.PersistentFlags().String("madison-key", "", "Madison API key")
	cmd.PersistentFlags().Bool("dry-run", false, "Show what would be done without executing")

	commandName := cmd.Name()
	config.AddCommandFlags(cmd, commandName)
}
