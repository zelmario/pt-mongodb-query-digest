package cmd

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/detector"
	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
	"github.com/zelmario/pt-mongodb-query-digest/internal/filter"
	"github.com/zelmario/pt-mongodb-query-digest/internal/fingerprint"
	"github.com/zelmario/pt-mongodb-query-digest/internal/parser"
	"github.com/zelmario/pt-mongodb-query-digest/internal/report"
	"github.com/zelmario/pt-mongodb-query-digest/internal/web"
)

type profileOpts struct {
	logOpts // shared flags
	db      string
}

func newProfileCmd() *cobra.Command {
	var o profileOpts
	cmd := &cobra.Command{
		Use:   "profile [URI]",
		Short: "Read system.profile from a running mongod.",
		Long: `Read and analyze system.profile documents from a MongoDB instance.
The URI can be passed as the positional argument or via MONGODB_URI.
Requires the profiler to be enabled on the target database(s).`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			uri := ""
			if len(args) == 1 {
				uri = args[0]
			}
			if uri == "" {
				uri = os.Getenv("MONGODB_URI")
			}
			if uri == "" {
				return fmt.Errorf("no MongoDB URI (pass as arg or set MONGODB_URI)")
			}
			return runProfile(c.Context(), uri, &o)
		},
	}

	f := cmd.Flags()
	// Reuse the same flag set as `log`.
	f.StringVarP(&o.output, "output", "o", "text", "output format: text|json")
	f.IntVar(&o.limit, "limit", 20, "max number of query classes to report")
	f.StringVar(&o.since, "since", "", "only include events since (relative like 2h, or RFC3339)")
	f.StringVar(&o.until, "until", "", "only include events until (relative or RFC3339)")
	f.StringSliceVar(&o.ns, "ns", nil, "include namespaces (glob, repeatable)")
	f.StringSliceVar(&o.notNs, "not-ns", nil, "exclude namespaces (glob, repeatable)")
	f.StringVar(&o.op, "op", "", "op types to include (CSV)")
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
	f.StringVar(&o.save, "save", "", "save this run to local history under NAME")
	f.StringVar(&o.db, "db", "", "database to read profiler from (defaults to all visible non-system dbs; or the db path in the URI)")
	return cmd
}

func runProfile(pctx context.Context, uri string, o *profileOpts) error {
	ctx, stop := signal.NotifyContext(pctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	now := time.Now()
	ef, err := buildEventFilter(&o.logOpts, now)
	if err != nil {
		return err
	}
	cf := buildClassFilter(&o.logOpts)

	// Resolve db from flag or URI path.
	dbName := o.db
	if dbName == "" {
		dbName = dbFromURI(uri)
	}

	popts := parser.ProfileOptions{
		URI:      uri,
		Database: dbName,
		Since:    bsonDateFromTime(ef.Since),
		Until:    bsonDateFromTime(ef.Until),
	}

	agg := aggregator.New()
	events := make(chan *event.Event, 1024)
	errc := make(chan error, 1)
	go func() { errc <- parser.ParseProfile(ctx, popts, events) }()

	for ev := range events {
		if !ef.Keep(ev) {
			continue
		}
		ev.Op, _, ev.Shape = fingerprint.Canonicalize(ev.Command)
		if ev.Op == "" {
			continue
		}
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
	if err := <-errc; err != nil {
		return fmt.Errorf("profile: %w", err)
	}

	sums := agg.Summaries()
	cfg := detector.DefaultConfig()
	for _, s := range sums {
		detector.Annotate(s, cfg)
	}
	kept := sums[:0]
	for _, s := range sums {
		if cf.Keep(s) {
			kept = append(kept, s)
		}
	}
	sums = kept

	rctx := report.Context{
		Source:      redactURI(uri),
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
		return fmt.Errorf("unknown output format %q", o.output)
	}
}

func dbFromURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	path := strings.TrimPrefix(u.Path, "/")
	if idx := strings.IndexByte(path, '/'); idx >= 0 {
		path = path[:idx]
	}
	return path
}

func redactURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	if u.User != nil {
		u.User = url.User(u.User.Username())
	}
	return u.String()
}

func bsonDateFromTime(t time.Time) bson.DateTime {
	if t.IsZero() {
		return 0
	}
	return bson.NewDateTimeFromTime(t)
}

func init() {
	_ = filter.ParseTime // keep compiler happy if unused after refactor; safe import anchor
}
