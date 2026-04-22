package cmd

import (
	"fmt"
	"math"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff A B",
		Short: "Compare two saved runs — shows regressions, improvements, new and gone classes.",
		Args:  cobra.ExactArgs(2),
		RunE:  runDiff,
	}
	cmd.Flags().Int("top", 15, "max classes to show in each section")
	return cmd
}

func runDiff(cmd *cobra.Command, args []string) error {
	top, _ := cmd.Flags().GetInt("top")
	if top <= 0 {
		top = 15
	}

	store, err := openHistory()
	if err != nil {
		return err
	}
	defer store.Close()

	_, sumsA, err := store.Load(args[0])
	if err != nil {
		return fmt.Errorf("load %s: %w", args[0], err)
	}
	_, sumsB, err := store.Load(args[1])
	if err != nil {
		return fmt.Errorf("load %s: %w", args[1], err)
	}

	byID := func(sums []*aggregator.Summary) map[string]*aggregator.Summary {
		m := make(map[string]*aggregator.Summary, len(sums))
		for _, s := range sums {
			m[s.ID] = s
		}
		return m
	}
	a := byID(sumsA)
	b := byID(sumsB)

	var (
		regressions []*diffEntry
		improvements []*diffEntry
		newClasses []*aggregator.Summary
		goneClasses []*aggregator.Summary
	)

	for id, sb := range b {
		sa, ok := a[id]
		if !ok {
			newClasses = append(newClasses, sb)
			continue
		}
		entry := &diffEntry{A: sa, B: sb,
			DeltaTotal: sb.TotalMs - sa.TotalMs,
			DeltaP95:   sb.P95Ms - sa.P95Ms,
			DeltaCount: sb.Count - sa.Count,
		}
		entry.FactorP95 = safeRatio(sb.P95Ms, sa.P95Ms)
		if sb.P95Ms > sa.P95Ms*1.2 {
			regressions = append(regressions, entry)
		} else if sa.P95Ms > sb.P95Ms*1.2 {
			improvements = append(improvements, entry)
		}
	}
	for id, sa := range a {
		if _, ok := b[id]; !ok {
			goneClasses = append(goneClasses, sa)
		}
	}

	sort.SliceStable(regressions, func(i, j int) bool {
		return regressions[i].DeltaP95 > regressions[j].DeltaP95
	})
	sort.SliceStable(improvements, func(i, j int) bool {
		return improvements[i].DeltaP95 < improvements[j].DeltaP95
	})
	sort.SliceStable(newClasses, func(i, j int) bool {
		return newClasses[i].TotalMs > newClasses[j].TotalMs
	})
	sort.SliceStable(goneClasses, func(i, j int) bool {
		return goneClasses[i].TotalMs > goneClasses[j].TotalMs
	})

	fmt.Printf("# Comparing %q (A) → %q (B)\n\n", args[0], args[1])
	printDiffTable("Regressions (B slower than A)", regressions, top, false)
	printDiffTable("Improvements (B faster than A)", improvements, top, true)
	printNewGoneTable("New classes in B", newClasses, top)
	printNewGoneTable("Gone (in A, not in B)", goneClasses, top)
	return nil
}

type diffEntry struct {
	A, B       *aggregator.Summary
	DeltaTotal float64
	DeltaP95   float64
	DeltaCount int64
	FactorP95  float64
}

func printDiffTable(title string, entries []*diffEntry, top int, improvement bool) {
	fmt.Printf("## %s: %d\n", title, len(entries))
	if len(entries) == 0 {
		fmt.Println()
		return
	}
	if len(entries) > top {
		entries = entries[:top]
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tOP\tNS\tP95 A\tP95 B\tΔP95\t×\tCALLS A→B")
	for _, e := range entries {
		factor := fmt.Sprintf("%.2f", e.FactorP95)
		if improvement && e.FactorP95 > 0 {
			factor = fmt.Sprintf("%.2f", 1/e.FactorP95)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d → %d\n",
			e.B.ID, e.B.Op, e.B.Namespace,
			fmtMsShort(e.A.P95Ms), fmtMsShort(e.B.P95Ms),
			fmtSignedMs(e.DeltaP95), factor,
			e.A.Count, e.B.Count)
	}
	tw.Flush()
	fmt.Println()
}

func printNewGoneTable(title string, sums []*aggregator.Summary, top int) {
	fmt.Printf("## %s: %d\n", title, len(sums))
	if len(sums) == 0 {
		fmt.Println()
		return
	}
	if len(sums) > top {
		sums = sums[:top]
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tOP\tNS\tCALLS\tTOTAL\tP95")
	for _, s := range sums {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n",
			s.ID, s.Op, s.Namespace, s.Count,
			fmtMsShort(s.TotalMs), fmtMsShort(s.P95Ms))
	}
	tw.Flush()
	fmt.Println()
}

func safeRatio(n, d float64) float64 {
	if d == 0 {
		return math.Inf(1)
	}
	return n / d
}

func fmtMsShort(ms float64) string {
	abs := ms
	if abs < 0 {
		abs = -abs
	}
	sign := ""
	if ms < 0 {
		sign = "-"
	}
	switch {
	case abs == 0:
		return "0"
	case abs < 1000:
		return fmt.Sprintf("%s%.0fms", sign, abs)
	case abs < 60_000:
		return fmt.Sprintf("%s%.1fs", sign, abs/1000)
	default:
		return fmt.Sprintf("%s%.1fm", sign, abs/60_000)
	}
}

func fmtSignedMs(ms float64) string {
	if ms > 0 {
		return "+" + fmtMsShort(ms)
	}
	return fmtMsShort(ms)
}
