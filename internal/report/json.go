package report

import (
	"encoding/json"
	"io"
	"sort"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
)

const SchemaVersion = "1.0.0"

type jsonReport struct {
	SchemaVersion string      `json:"schema_version"`
	Source        string      `json:"source"`
	GeneratedAt   time.Time   `json:"generated_at"`
	Earliest      *time.Time  `json:"earliest,omitempty"`
	Latest        *time.Time  `json:"latest,omitempty"`
	TotalEvents   int64       `json:"total_events"`
	TotalMs       float64     `json:"total_ms"`
	Classes       []jsonClass `json:"classes"`
}

type jsonClass struct {
	ID              string    `json:"id"`
	Op              string    `json:"op"`
	Namespace       string    `json:"namespace"`
	Shape           string    `json:"shape"`
	Count           int64     `json:"count"`
	FirstSeen       *time.Time `json:"first_seen,omitempty"`
	LastSeen        *time.Time `json:"last_seen,omitempty"`
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
}

func WriteJSON(w io.Writer, ctx Context, sums []*aggregator.Summary, limit int) error {
	sort.SliceStable(sums, func(i, j int) bool { return sums[i].TotalMs > sums[j].TotalMs })
	if limit > 0 && len(sums) > limit {
		sums = sums[:limit]
	}
	out := jsonReport{
		SchemaVersion: SchemaVersion,
		Source:        ctx.Source,
		GeneratedAt:   ctx.StartedAt,
		TotalEvents:   ctx.TotalEvents,
		TotalMs:       ctx.TotalMs,
	}
	if !ctx.Earliest.IsZero() {
		t := ctx.Earliest
		out.Earliest = &t
	}
	if !ctx.Latest.IsZero() {
		t := ctx.Latest
		out.Latest = &t
	}
	out.Classes = make([]jsonClass, 0, len(sums))
	for _, s := range sums {
		jc := jsonClass{
			ID: s.ID, Op: s.Op, Namespace: s.Namespace, Shape: s.Shape,
			Count: s.Count, TotalMs: s.TotalMs,
			MinMs: s.MinMs, MaxMs: s.MaxMs, AvgMs: s.AvgMs,
			MedianMs: s.MedianMs, P95Ms: s.P95Ms, P99Ms: s.P99Ms,
			StddevMs:        s.StddevMs,
			AvgDocsExam:     s.AvgDocsExam,
			AvgDocsRet:      s.AvgDocsRet,
			AvgKeysExam:     s.AvgKeysExam,
			ExamReturnRatio: s.ExamReturnRatio,
			PlanSummary:     s.PlanSummary,
			DistinctPlans:   s.DistinctPlans,
			QueryHash:       s.QueryHash,
			Flags:           s.Flags,
		}
		if !s.FirstSeen.IsZero() {
			t := s.FirstSeen
			jc.FirstSeen = &t
		}
		if !s.LastSeen.IsZero() {
			t := s.LastSeen
			jc.LastSeen = &t
		}
		out.Classes = append(out.Classes, jc)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
