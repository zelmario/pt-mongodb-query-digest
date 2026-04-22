package aggregator

import (
	"math"
	"testing"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
)

func mk(class string, ms int64, ts time.Time) *event.Event {
	return &event.Event{
		ClassID:      class,
		Op:           "find",
		Namespace:    "db.coll",
		Shape:        `{"filter":{"x":"?"}}`,
		DurationMs:   ms,
		DocsExamined: ms * 10,
		DocsReturned: 1,
		Timestamp:    ts,
	}
}

func TestAggregator_GroupsByClassID(t *testing.T) {
	a := New()
	now := time.Now()
	a.Add(mk("A", 10, now))
	a.Add(mk("A", 20, now.Add(time.Second)))
	a.Add(mk("B", 5, now))
	sums := a.Summaries()
	if len(sums) != 2 {
		t.Fatalf("want 2 classes, got %d", len(sums))
	}
}

func TestAggregator_Percentiles(t *testing.T) {
	a := New()
	now := time.Now()
	for i := 1; i <= 100; i++ {
		a.Add(mk("A", int64(i), now))
	}
	sums := a.Summaries()
	s := sums[0]
	if math.Abs(s.MedianMs-50.5) > 2 {
		t.Errorf("median ~50.5 expected, got %v", s.MedianMs)
	}
	if s.P95Ms < 93 || s.P95Ms > 97 {
		t.Errorf("p95 ~95 expected, got %v", s.P95Ms)
	}
	if s.MinMs != 1 || s.MaxMs != 100 {
		t.Errorf("min/max wrong: %v/%v", s.MinMs, s.MaxMs)
	}
}

func TestAggregator_ExamReturnRatio(t *testing.T) {
	a := New()
	ev := &event.Event{
		ClassID: "A", Op: "find", Namespace: "db.c", Shape: "{}",
		DurationMs: 1, DocsExamined: 1000, DocsReturned: 10,
	}
	a.Add(ev)
	sums := a.Summaries()
	s := sums[0]
	if s.ExamReturnRatio != 100 {
		t.Errorf("ratio: got %v", s.ExamReturnRatio)
	}
}

func TestAggregator_TimeBounds(t *testing.T) {
	a := New()
	t0 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	a.Add(mk("A", 1, t0.Add(time.Hour)))
	a.Add(mk("A", 1, t0))
	a.Add(mk("A", 1, t0.Add(30*time.Minute)))
	sums := a.Summaries()
	if !sums[0].FirstSeen.Equal(t0) {
		t.Errorf("FirstSeen wrong: %v", sums[0].FirstSeen)
	}
	if !sums[0].LastSeen.Equal(t0.Add(time.Hour)) {
		t.Errorf("LastSeen wrong: %v", sums[0].LastSeen)
	}
}
