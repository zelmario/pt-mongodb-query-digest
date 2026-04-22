package detector

import (
	"strings"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
)

// Flag is an anti-pattern name. Kept short for CLI filtering.
type Flag string

const (
	FlagCollScan        Flag = "collscan"
	FlagLargeIn         Flag = "large-in"
	FlagUnanchoredRegex Flag = "unanchored-regex"
	FlagPlanFlip        Flag = "plan-flip"
	FlagWhere           Flag = "where"
	FlagNegation        Flag = "negation"
	FlagUnboundedSkip   Flag = "unbounded-skip"
)

// Description yields a one-line "why this is bad" plus a remediation hint.
type Description struct {
	Title  string
	Why    string
	Fix    string
}

var descriptions = map[Flag]Description{
	FlagCollScan: {
		Title: "Collection scan",
		Why:   "MongoDB read every document in the collection to answer this query.",
		Fix:   "Create an index on the filter / sort fields (ESR rule: equality, range, sort).",
	},
	FlagLargeIn: {
		Title: "Large $in array",
		Why:   "Huge $in lists force per-element index probes and inflate plan cost.",
		Fix:   "Batch the values, or reshape the query to use a range or a join.",
	},
	FlagUnanchoredRegex: {
		Title: "Unanchored regex",
		Why:   "Regex without a left anchor (^) cannot use an index prefix.",
		Fix:   "Anchor the pattern with ^, use a case-insensitive collation index, or a text index.",
	},
	FlagPlanFlip: {
		Title: "Plan instability",
		Why:   "This class uses more than one plan cache key — plan flips cause latency spikes.",
		Fix:   "Add a hint, or simplify the predicate so the planner picks one plan.",
	},
	FlagWhere: {
		Title: "$where (JS eval)",
		Why:   "$where runs JavaScript on each document and cannot use indexes.",
		Fix:   "Replace with a native operator or an aggregation pipeline.",
	},
	FlagNegation: {
		Title: "Negation operator",
		Why:   "$ne / $nin typically scan the index instead of seeking; often worse than a scan.",
		Fix:   "Reframe as a positive predicate, or add a partial/filtered index.",
	},
	FlagUnboundedSkip: {
		Title: "Unbounded skip",
		Why:   "skip() walks and discards N documents, so cost grows linearly with the page number.",
		Fix:   "Use keyset pagination (filter on the last seen id / sort key).",
	},
}

func Describe(f Flag) Description { return descriptions[f] }

// Config holds detector thresholds.
type Config struct {
	LargeInThreshold       int
	UnboundedSkipThreshold int64
}

func DefaultConfig() Config {
	return Config{
		LargeInThreshold:       200,
		UnboundedSkipThreshold: 1000,
	}
}

// Annotate fills s.Flags based on aggregated class info plus the sample event.
// Runs on the already-normalized shape string (operators preserved) and on the
// raw sample command when we have it.
func Annotate(s *aggregator.Summary, cfg Config) {
	var flags []Flag

	if strings.HasPrefix(s.PlanSummary, "COLLSCAN") {
		flags = append(flags, FlagCollScan)
	}
	if s.DistinctPlans > 1 {
		flags = append(flags, FlagPlanFlip)
	}
	if strings.Contains(s.Shape, `"$where"`) {
		flags = append(flags, FlagWhere)
	}
	if strings.Contains(s.Shape, `"$ne"`) || strings.Contains(s.Shape, `"$nin"`) {
		flags = append(flags, FlagNegation)
	}

	if s.Sample != nil && s.Sample.Command != nil {
		if detectUnanchoredRegex(s.Sample.Command) {
			flags = append(flags, FlagUnanchoredRegex)
		}
		if detectLargeIn(s.Sample.Command, cfg.LargeInThreshold) {
			flags = append(flags, FlagLargeIn)
		}
		if detectUnboundedSkip(s.Sample.Command, cfg.UnboundedSkipThreshold) {
			flags = append(flags, FlagUnboundedSkip)
		}
	}

	s.Flags = make([]string, len(flags))
	for i, f := range flags {
		s.Flags[i] = string(f)
	}
}

func detectUnanchoredRegex(cmd map[string]any) bool {
	var found bool
	walk(cmd, func(k string, v any) {
		if found {
			return
		}
		if k != "$regex" {
			return
		}
		switch pattern := v.(type) {
		case string:
			if !strings.HasPrefix(pattern, "^") {
				found = true
			}
		case map[string]any:
			if s, ok := pattern["$regularExpression"].(map[string]any); ok {
				if p, ok := s["pattern"].(string); ok && !strings.HasPrefix(p, "^") {
					found = true
				}
			}
		}
	})
	// Also BSON-style `{$regularExpression: {pattern: "...", options: "..."}}`.
	walk(cmd, func(k string, v any) {
		if found || k != "$regularExpression" {
			return
		}
		m, ok := v.(map[string]any)
		if !ok {
			return
		}
		if p, ok := m["pattern"].(string); ok && !strings.HasPrefix(p, "^") {
			found = true
		}
	})
	return found
}

func detectLargeIn(cmd map[string]any, threshold int) bool {
	var found bool
	walk(cmd, func(k string, v any) {
		if found {
			return
		}
		if k != "$in" && k != "$nin" {
			return
		}
		if arr, ok := v.([]any); ok && len(arr) > threshold {
			found = true
		}
	})
	return found
}

func detectUnboundedSkip(cmd map[string]any, threshold int64) bool {
	v, ok := cmd["skip"]
	if !ok {
		return false
	}
	switch n := v.(type) {
	case float64:
		return int64(n) > threshold
	case int64:
		return n > threshold
	case int:
		return int64(n) > threshold
	}
	return false
}

// walk invokes fn(key, value) for every key/value pair encountered while
// recursing through nested maps and arrays.
func walk(v any, fn func(key string, value any)) {
	switch val := v.(type) {
	case map[string]any:
		for k, sub := range val {
			fn(k, sub)
			walk(sub, fn)
		}
	case []any:
		for _, e := range val {
			walk(e, fn)
		}
	}
}
