package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pt-mongodb-query-digest",
	Short: "Analyze MongoDB slow queries from a log file or the profiler.",
	Long: `pt-mongodb-query-digest groups MongoDB operations by shape, surfaces the
worst offenders, and flags common anti-patterns (COLLSCAN, unanchored regex,
large $in, plan flips, etc.).`,
	SilenceUsage: true,
}

func Execute() error { return rootCmd.Execute() }

func init() {
	rootCmd.PersistentFlags().StringVar(&historyDBFlag, "history-db", "",
		"path to the history SQLite file (default: ~/.local/share/pt-mongodb-query-digest/history.db)")
	rootCmd.AddCommand(newLogCmd())
	rootCmd.AddCommand(newProfileCmd())
	rootCmd.AddCommand(newHistoryCmd())
	rootCmd.AddCommand(newDiffCmd())
}
