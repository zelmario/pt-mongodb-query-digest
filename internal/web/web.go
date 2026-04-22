// Package web renders the pt-mongodb-query-digest report as HTML, either as
// a static file (for --html) or served over HTTP (for --web). All assets are
// embedded; no CDN calls, no external dependencies at runtime.
package web

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"sort"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/detector"
	"github.com/zelmario/pt-mongodb-query-digest/internal/report"
)

//go:embed assets/index.html
var tmplSrc string

//go:embed assets/app.css
var cssSrc string

//go:embed assets/app.js
var jsSrc string

var pageTmpl = template.Must(template.New("page").Parse(tmplSrc))

type pageData struct {
	Source      string
	GeneratedAt time.Time
	Earliest    time.Time
	Latest      time.Time
	TotalEvents int64
	TotalMsFmt  string
	Classes     []classData
	JSON        template.JS
	JS          template.JS
	CSS         template.CSS
}

type classData struct {
	ID              string    `json:"id"`
	Op              string    `json:"op"`
	Namespace       string    `json:"namespace"`
	Shape           string    `json:"shape"`
	Count           int64     `json:"count"`
	FirstSeen       string    `json:"first_seen,omitempty"`
	LastSeen        string    `json:"last_seen,omitempty"`
	TotalMs         float64   `json:"total_ms"`
	MinMs           float64   `json:"min_ms"`
	MaxMs           float64   `json:"max_ms"`
	AvgMs           float64   `json:"avg_ms"`
	MedianMs        float64   `json:"median_ms"`
	P95Ms           float64   `json:"p95_ms"`
	P99Ms           float64   `json:"p99_ms"`
	StddevMs        float64   `json:"stddev_ms"`
	AvgDocsExam     float64   `json:"avg_docs_examined"`
	AvgDocsRet      float64   `json:"avg_docs_returned"`
	AvgKeysExam     float64   `json:"avg_keys_examined"`
	ExamReturnRatio float64   `json:"exam_return_ratio"`
	PlanSummary     string    `json:"plan_summary,omitempty"`
	DistinctPlans   int       `json:"distinct_plans"`
	QueryHash       string    `json:"query_hash,omitempty"`
	Flags           []string  `json:"flags,omitempty"`
	Histogram       []int     `json:"histogram"`
}

type payload struct {
	SchemaVersion    string                         `json:"schema_version"`
	Source           string                         `json:"source"`
	GeneratedAt      time.Time                      `json:"generated_at"`
	TotalEvents      int64                          `json:"total_events"`
	TotalMs          float64                        `json:"total_ms"`
	Classes          []classData                    `json:"classes"`
	FlagDescriptions map[string]detector.Description `json:"flag_descriptions"`
}

// Render produces the full self-contained HTML for a report.
func Render(ctx report.Context, sums []*aggregator.Summary, limit int) ([]byte, error) {
	classes := buildClasses(sums, limit)

	p := payload{
		SchemaVersion:    report.SchemaVersion,
		Source:           ctx.Source,
		GeneratedAt:      ctx.StartedAt,
		TotalEvents:      ctx.TotalEvents,
		TotalMs:          ctx.TotalMs,
		Classes:          classes,
		FlagDescriptions: allFlagDescriptions(),
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	pd := pageData{
		Source:      ctx.Source,
		GeneratedAt: ctx.StartedAt,
		Earliest:    ctx.Earliest,
		Latest:      ctx.Latest,
		TotalEvents: ctx.TotalEvents,
		TotalMsFmt:  fmtMs(ctx.TotalMs),
		Classes:     classes,
		JSON:        template.JS(data),
		JS:          template.JS(jsSrc),
		CSS:         template.CSS(cssSrc),
	}
	var buf bytes.Buffer
	if err := pageTmpl.Execute(&buf, pd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func buildClasses(sums []*aggregator.Summary, limit int) []classData {
	sort.SliceStable(sums, func(i, j int) bool { return sums[i].TotalMs > sums[j].TotalMs })
	if limit > 0 && len(sums) > limit {
		sums = sums[:limit]
	}
	out := make([]classData, 0, len(sums))
	for _, s := range sums {
		cd := classData{
			ID: s.ID, Op: s.Op, Namespace: s.Namespace, Shape: s.Shape,
			Count:   s.Count,
			TotalMs: s.TotalMs, MinMs: s.MinMs, MaxMs: s.MaxMs, AvgMs: s.AvgMs,
			MedianMs: s.MedianMs, P95Ms: s.P95Ms, P99Ms: s.P99Ms, StddevMs: s.StddevMs,
			AvgDocsExam:     s.AvgDocsExam,
			AvgDocsRet:      s.AvgDocsRet,
			AvgKeysExam:     s.AvgKeysExam,
			ExamReturnRatio: s.ExamReturnRatio,
			PlanSummary:     s.PlanSummary,
			DistinctPlans:   s.DistinctPlans,
			QueryHash:       s.QueryHash,
			Flags:           s.Flags,
			Histogram:       s.Histogram,
		}
		if !s.FirstSeen.IsZero() {
			cd.FirstSeen = s.FirstSeen.Format(time.RFC3339)
		}
		if !s.LastSeen.IsZero() {
			cd.LastSeen = s.LastSeen.Format(time.RFC3339)
		}
		out = append(out, cd)
	}
	return out
}

func allFlagDescriptions() map[string]detector.Description {
	flags := []detector.Flag{
		detector.FlagCollScan, detector.FlagLargeIn, detector.FlagUnanchoredRegex,
		detector.FlagPlanFlip, detector.FlagWhere, detector.FlagNegation, detector.FlagUnboundedSkip,
	}
	out := make(map[string]detector.Description, len(flags))
	for _, f := range flags {
		out[string(f)] = detector.Describe(f)
	}
	return out
}

func fmtMs(ms float64) string {
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
