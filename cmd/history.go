package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/zelmario/pt-mongodb-query-digest/internal/history"
	"github.com/zelmario/pt-mongodb-query-digest/internal/report"
)

var historyDBFlag string

func newHistoryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history",
		Short: "List, show, and delete saved analysis runs.",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List saved runs.",
		RunE:  runHistoryList,
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "show NAME",
		Short: "Print a saved run as a text report.",
		Args:  cobra.ExactArgs(1),
		RunE:  runHistoryShow,
	})
	cmd.AddCommand(&cobra.Command{
		Use:     "rm NAME",
		Aliases: []string{"delete", "remove"},
		Short:   "Delete a saved run.",
		Args:    cobra.ExactArgs(1),
		RunE:    runHistoryRm,
	})
	return cmd
}

func openHistory() (*history.Store, error) {
	path := historyDBFlag
	if path == "" {
		p, err := history.DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}
	return history.Open(path)
}

func runHistoryList(cmd *cobra.Command, args []string) error {
	store, err := openHistory()
	if err != nil {
		return err
	}
	defer store.Close()

	runs, err := store.List()
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("(no saved runs)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSAVED\tCLASSES\tEVENTS\tSOURCE")
	for _, r := range runs {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\n",
			r.Name,
			r.SavedAt.In(time.Local).Format("2006-01-02 15:04"),
			r.ClassCount, r.TotalEvents, r.Source)
	}
	return tw.Flush()
}

func runHistoryShow(cmd *cobra.Command, args []string) error {
	store, err := openHistory()
	if err != nil {
		return err
	}
	defer store.Close()

	rctx, sums, err := store.Load(args[0])
	if err != nil {
		return err
	}
	return report.WriteText(os.Stdout, rctx, sums, 0)
}

func runHistoryRm(cmd *cobra.Command, args []string) error {
	store, err := openHistory()
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Delete(args[0]); err != nil {
		return err
	}
	fmt.Printf("deleted run %q\n", args[0])
	return nil
}
