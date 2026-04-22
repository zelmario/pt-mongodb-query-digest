package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/detector"
	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
	"github.com/zelmario/pt-mongodb-query-digest/internal/filter"
	"github.com/zelmario/pt-mongodb-query-digest/internal/fingerprint"
	"github.com/zelmario/pt-mongodb-query-digest/internal/history"
	"github.com/zelmario/pt-mongodb-query-digest/internal/parser"
	"github.com/zelmario/pt-mongodb-query-digest/internal/report"
	"github.com/zelmario/pt-mongodb-query-digest/internal/web"
)

type logOpts struct {
	output   string
	limit    int
	since    string
	until    string
	ns       []string
	notNs    []string
	op       string
	notOp    string
	app      []string
	user     []string
	minMs    int64
	minCount int64
	flags    string
	notFlags string
	web      bool
	webAddr  string
	noOpen   bool
	html     string
	save     string
}

func newLogCmd() *cobra.Command {
	var o logOpts
	cmd := &cobra.Command{
		Use:   "log [FILE]",
		Short: "Analyze a mongod JSON log file (or stdin when FILE is -).",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			path := "-"
			if len(args) == 1 {
				path = args[0]
			}
			return runLog(c.Context(), path, &o)
		},
	}

	f := cmd.Flags()
	f.StringVarP(&o.output, "output", "o", "text", "output format: text|json")
	f.IntVar(&o.limit, "limit", 20, "max number of query classes to report")
	f.StringVar(&o.since, "since", "", "only include events since (relative like 2h, or RFC3339)")
	f.StringVar(&o.until, "until", "", "only include events until (relative or RFC3339)")
	f.StringSliceVar(&o.ns, "ns", nil, "include namespaces (glob, repeatable)")
	f.StringSliceVar(&o.notNs, "not-ns", nil, "exclude namespaces (glob, repeatable)")
	f.StringVar(&o.op, "op", "", "op types to include (CSV: find,aggregate,...)")
	f.StringVar(&o.notOp, "not-op", "", "op types to exclude (CSV)")
	f.StringSliceVar(&o.app, "app", nil, "include appName (glob, repeatable)")
	f.StringSliceVar(&o.user, "user", nil, "include user (glob, repeatable)")
	f.Int64Var(&o.minMs, "min-ms", 0, "drop events faster than this")
	f.Int64Var(&o.minCount, "min-count", 0, "drop classes with fewer than N calls")
	f.StringVar(&o.flags, "flag", "", "only keep classes with these flags (CSV)")
	f.StringVar(&o.notFlags, "not-flag", "", "drop classes with these flags (CSV)")
	f.BoolVar(&o.web, "web", false, "serve the report as a local web UI")
	f.StringVar(&o.webAddr, "web-addr", "127.0.0.1:8080", "bind address for --web")
	f.BoolVar(&o.noOpen, "no-open", false, "don't auto-launch the browser with --web")
	f.StringVar(&o.html, "html", "", "write a self-contained HTML report to FILE and exit")
	f.StringVar(&o.save, "save", "", "save this run to local history under NAME (for `diff` / `history` later)")
	return cmd
}

func runLog(pctx context.Context, path string, o *logOpts) error {
	ctx, stop := signal.NotifyContext(pctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	now := time.Now()
	ef, err := buildEventFilter(o, now)
	if err != nil {
		return err
	}
	cf := buildClassFilter(o)

	r, err := parser.OpenLog(path)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer r.Close()

	agg := aggregator.New()
	events := make(chan *event.Event, 1024)
	errc := make(chan error, 1)
	go func() { errc <- parser.ParseLog(ctx, r, events); close(events) }()

	for ev := range events {
		if !ef.Keep(ev) {
			continue
		}
		var coll string
		ev.Op, coll, ev.Shape = fingerprint.Canonicalize(ev.Command)
		if ev.Op == "" {
			continue
		}
		// MongoDB logs batch writes with ns="<db>.$cmd"; the real collection
		// is the value of the op key inside the command body. Prefer that.
		if coll != "" && strings.HasSuffix(ev.Namespace, ".$cmd") {
			ev.Namespace = ev.Database + "." + coll
			ev.Collection = coll
		}
		// Filter by op here (after fingerprinting establishes the op).
		if len(ef.Ops) > 0 {
			if _, ok := ef.Ops[ev.Op]; !ok {
				continue
			}
		}
		if len(ef.NotOps) > 0 {
			if _, ok := ef.NotOps[ev.Op]; ok {
				continue
			}
		}
		ev.ClassID = fingerprint.ClassID(ev.Op, ev.Namespace, ev.Shape)
		agg.Add(ev)
	}
	if err := <-errc; err != nil && !isStdinClosed(err) {
		return fmt.Errorf("parse log: %w", err)
	}

	sums := agg.Summaries()
	cfg := detector.DefaultConfig()
	for _, s := range sums {
		detector.Annotate(s, cfg)
	}
	// Apply class-level filter after annotation.
	kept := sums[:0]
	for _, s := range sums {
		if cf.Keep(s) {
			kept = append(kept, s)
		}
	}
	sums = kept

	rctx := report.Context{
		Source:      path,
		StartedAt:   now,
		Earliest:    agg.Earliest(),
		Latest:      agg.Latest(),
		TotalEvents: agg.TotalEvents(),
		TotalMs:     agg.TotalMs(),
	}

	if o.save != "" {
		if err := saveRun(o.save, rctx, sums); err != nil {
			return err
		}
	}
	if o.html != "" {
		html, err := web.Render(rctx, sums, o.limit)
		if err != nil {
			return fmt.Errorf("render html: %w", err)
		}
		if err := os.WriteFile(o.html, html, 0o644); err != nil {
			return fmt.Errorf("write html: %w", err)
		}
		fmt.Fprintf(os.Stderr, "wrote %s (%d bytes)\n", o.html, len(html))
		return nil
	}
	if o.web {
		return web.Serve(ctx, o.webAddr, !o.noOpen, rctx, sums, o.limit)
	}

	var out io.Writer = os.Stdout
	switch strings.ToLower(o.output) {
	case "text", "":
		return report.WriteText(out, rctx, sums, o.limit)
	case "json":
		return report.WriteJSON(out, rctx, sums, o.limit)
	default:
		return fmt.Errorf("unknown output format %q (supported: text, json)", o.output)
	}
}

func buildEventFilter(o *logOpts, now time.Time) (*filter.EventFilter, error) {
	since, err := filter.ParseTime(o.since, now)
	if err != nil {
		return nil, fmt.Errorf("--since: %w", err)
	}
	until, err := filter.ParseTime(o.until, now)
	if err != nil {
		return nil, fmt.Errorf("--until: %w", err)
	}
	return &filter.EventFilter{
		Since:      since,
		Until:      until,
		NSGlobs:    o.ns,
		NotNSGlobs: o.notNs,
		Ops:        filter.ParseOps(o.op),
		NotOps:     filter.ParseOps(o.notOp),
		Apps:       o.app,
		Users:      o.user,
		MinMs:      o.minMs,
	}, nil
}

func buildClassFilter(o *logOpts) *filter.ClassFilter {
	return &filter.ClassFilter{
		MinCount: o.minCount,
		Flags:    splitCSV(o.flags),
		NotFlags: splitCSV(o.notFlags),
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func isStdinClosed(err error) bool {
	// bufio.Scanner on a closed stdin occasionally surfaces io.EOF; treat as normal.
	return err == io.EOF
}

func saveRun(name string, rctx report.Context, sums []*aggregator.Summary) error {
	path := historyDBFlag
	if path == "" {
		p, err := history.DefaultPath()
		if err != nil {
			return fmt.Errorf("resolve history path: %w", err)
		}
		path = p
	}
	store, err := history.Open(path)
	if err != nil {
		return fmt.Errorf("open history: %w", err)
	}
	defer store.Close()
	if err := store.Save(name, rctx, sums); err != nil {
		return fmt.Errorf("save run %q: %w", name, err)
	}
	fmt.Fprintf(os.Stderr, "saved run %q (%d classes) to %s\n", name, len(sums), path)
	return nil
}
