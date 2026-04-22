package report

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/detector"
)

// Context carries corpus-level info shown in the report header.
type Context struct {
	Source      string
	StartedAt   time.Time
	Earliest    time.Time
	Latest      time.Time
	TotalEvents int64
	TotalMs     float64
}

// WriteText renders a pt-query-digest-style static report.
func WriteText(w io.Writer, ctx Context, sums []*aggregator.Summary, limit int) error {
	sort.SliceStable(sums, func(i, j int) bool { return sums[i].TotalMs > sums[j].TotalMs })
	if limit > 0 && len(sums) > limit {
		sums = sums[:limit]
	}

	bw := &errWriter{w: w}
	writeHeader(bw, ctx, sums)
	writeProfile(bw, ctx, sums)
	for rank, s := range sums {
		writeClass(bw, rank+1, s)
	}
	return bw.err
}

func writeHeader(w *errWriter, ctx Context, sums []*aggregator.Summary) {
	w.printf("# pt-mongodb-query-digest report\n")
	w.printf("# Source:        %s\n", ctx.Source)
	w.printf("# Generated:     %s\n", ctx.StartedAt.Format(time.RFC3339))
	if !ctx.Earliest.IsZero() {
		w.printf("# Event range:   %s .. %s\n",
			ctx.Earliest.Format(time.RFC3339),
			ctx.Latest.Format(time.RFC3339))
	}
	w.printf("# Total events:  %d\n", ctx.TotalEvents)
	w.printf("# Unique shapes: %d\n", len(sums))
	w.printf("# Total exec:    %s\n\n", formatMs(ctx.TotalMs))
}

func writeProfile(w *errWriter, ctx Context, sums []*aggregator.Summary) {
	if len(sums) == 0 {
		w.printf("# No events matched.\n")
		return
	}
	w.printf("# Profile\n")
	w.printf("# %-4s %-16s %-10s %10s %10s %10s %s\n",
		"Rank", "ID", "Op", "Calls", "Total", "95%/call", "Namespace")
	w.printf("# %s\n", strings.Repeat("=", 78))
	for rank, s := range sums {
		w.printf("# %-4d %-16s %-10s %10d %10s %10s %s\n",
			rank+1, s.ID, s.Op, s.Count,
			formatMs(s.TotalMs), formatMs(s.P95Ms),
			truncate(s.Namespace, 30))
	}
	w.printf("\n")
}

func writeClass(w *errWriter, rank int, s *aggregator.Summary) {
	w.printf("# Query %d: %d calls, total %s, avg %s, p95 %s, max %s, ID %s\n",
		rank, s.Count, formatMs(s.TotalMs), formatMs(s.AvgMs),
		formatMs(s.P95Ms), formatMs(s.MaxMs), s.ID)
	w.printf("# Namespace:  %s\n", s.Namespace)
	w.printf("# Op:         %s\n", s.Op)
	if s.PlanSummary != "" {
		w.printf("# Plan:       %s", s.PlanSummary)
		if s.DistinctPlans > 1 {
			w.printf(" (%d distinct plans)", s.DistinctPlans)
		}
		w.printf("\n")
	}
	if s.QueryHash != "" {
		w.printf("# queryHash:  %s\n", s.QueryHash)
	}
	if !s.FirstSeen.IsZero() {
		w.printf("# Time range: %s .. %s\n",
			s.FirstSeen.Format(time.RFC3339),
			s.LastSeen.Format(time.RFC3339))
	}
	w.printf("# Duration:   min %s  median %s  p95 %s  p99 %s  stddev %s\n",
		formatMs(s.MinMs), formatMs(s.MedianMs),
		formatMs(s.P95Ms), formatMs(s.P99Ms), formatMs(s.StddevMs))
	w.printf("# Docs:       examined/call %.1f  returned/call %.1f  keys/call %.1f  ratio %s\n",
		s.AvgDocsExam, s.AvgDocsRet, s.AvgKeysExam, formatRatio(s.ExamReturnRatio))

	if len(s.Flags) > 0 {
		w.printf("#\n# Flags:\n")
		for _, f := range s.Flags {
			d := detector.Describe(detector.Flag(f))
			if d.Title != "" {
				w.printf("#   * %-20s  %s\n", f, d.Why)
				w.printf("#     fix:                %s\n", d.Fix)
			} else {
				w.printf("#   * %s\n", f)
			}
		}
	}

	w.printf("#\n# Shape:\n%s\n\n", indent(s.Shape, "  "))
}

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, a ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, a...)
}

func formatMs(ms float64) string {
	switch {
	case ms <= 0:
		return "0"
	case ms < 1:
		return fmt.Sprintf("%.0fus", ms*1000)
	case ms < 1000:
		return fmt.Sprintf("%.1fms", ms)
	case ms < 60_000:
		return fmt.Sprintf("%.2fs", ms/1000)
	default:
		return fmt.Sprintf("%.1fm", ms/60_000)
	}
}

func formatRatio(r float64) string {
	if r == 0 {
		return "-"
	}
	if r > 1e17 {
		return "inf"
	}
	return fmt.Sprintf("%.1f:1", r)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func indent(s, pad string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = pad + line
	}
	return strings.Join(lines, "\n")
}
