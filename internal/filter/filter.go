package filter

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
)

// EventFilter decides whether an Event survives into aggregation.
type EventFilter struct {
	Since   time.Time
	Until   time.Time
	NSGlobs []string
	NotNSGlobs []string
	Ops     map[string]struct{}
	NotOps  map[string]struct{}
	Apps    []string
	Users   []string
	MinMs   int64
}

func (f *EventFilter) Keep(ev *event.Event) bool {
	if !f.Since.IsZero() && !ev.Timestamp.IsZero() && ev.Timestamp.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && !ev.Timestamp.IsZero() && !ev.Timestamp.Before(f.Until) {
		return false
	}
	if ev.DurationMs < f.MinMs {
		return false
	}
	if len(f.NSGlobs) > 0 && !matchAny(f.NSGlobs, ev.Namespace) {
		return false
	}
	if len(f.NotNSGlobs) > 0 && matchAny(f.NotNSGlobs, ev.Namespace) {
		return false
	}
	if len(f.Ops) > 0 {
		if _, ok := f.Ops[ev.Op]; !ok {
			return false
		}
	}
	if len(f.NotOps) > 0 {
		if _, ok := f.NotOps[ev.Op]; ok {
			return false
		}
	}
	if len(f.Apps) > 0 && !matchAny(f.Apps, ev.AppName) {
		return false
	}
	if len(f.Users) > 0 && !matchAny(f.Users, ev.User) {
		return false
	}
	return true
}

// ClassFilter runs after aggregation + anti-pattern detection.
type ClassFilter struct {
	MinCount int64
	Flags    []string
	NotFlags []string
}

func (f *ClassFilter) Keep(s *aggregator.Summary) bool {
	if s.Count < f.MinCount {
		return false
	}
	if len(f.Flags) > 0 && !hasAnyFlag(s.Flags, f.Flags) {
		return false
	}
	if len(f.NotFlags) > 0 && hasAnyFlag(s.Flags, f.NotFlags) {
		return false
	}
	return true
}

func hasAnyFlag(have, want []string) bool {
	for _, w := range want {
		for _, h := range have {
			if h == w {
				return true
			}
		}
	}
	return false
}

func matchAny(globs []string, s string) bool {
	if s == "" {
		return false
	}
	for _, g := range globs {
		if ok, _ := filepath.Match(g, s); ok {
			return true
		}
	}
	return false
}

// ParseOps converts a CSV flag value into a set.
func ParseOps(csv string) map[string]struct{} {
	if csv == "" {
		return nil
	}
	out := make(map[string]struct{})
	for _, s := range strings.Split(csv, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out[normalizeOp(s)] = struct{}{}
	}
	return out
}

func normalizeOp(s string) string {
	switch strings.ToLower(s) {
	case "findandmodify":
		return "findAndModify"
	case "getmore":
		return "getMore"
	default:
		return strings.ToLower(s)
	}
}

// ParseDuration parses either a plain relative duration like "2h" (→ ago) or
// an RFC3339 absolute timestamp. now() is the reference for relative values.
func ParseTime(s string, now time.Time) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return now.Add(-d), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05Z", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time format: %s", s)
}
