package aggregator

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
)

// DefaultReservoirSize caps per-class samples used for percentiles. Memory
// cost: 8 bytes per sample per class. 10k × 1k classes ≈ 80 MB worst case.
const DefaultReservoirSize = 10000

// Class holds aggregated metrics for one query shape.
type Class struct {
	ID             string
	Op             string
	Namespace      string
	Shape          string
	Count          int64
	FirstSeen      time.Time
	LastSeen       time.Time
	SumMs          float64
	SumSqMs        float64
	MinMs          float64
	MaxMs          float64
	SumDocsExam    int64
	SumKeysExam    int64
	SumDocsRet     int64
	SumYields      int64
	SumRespLen     int64
	PlanSummaries  map[string]int64
	PlanCacheKeys  map[string]struct{}
	QueryHashes    map[string]struct{}
	AppNames       map[string]int64
	Users          map[string]int64
	Namespaces     map[string]int64
	Sample         *event.Event
	durationSamples []float64
	seen            int64
	rng             *rand.Rand
}

// Summary is the computed per-class stats used for reporting.
type Summary struct {
	ID            string
	Op            string
	Namespace     string
	Shape         string
	Count         int64
	FirstSeen     time.Time
	LastSeen      time.Time
	TotalMs       float64
	MinMs         float64
	MaxMs         float64
	AvgMs         float64
	MedianMs      float64
	P95Ms         float64
	P99Ms         float64
	StddevMs      float64
	AvgDocsExam   float64
	AvgDocsRet    float64
	AvgKeysExam   float64
	ExamReturnRatio float64
	PlanSummary   string
	DistinctPlans int
	QueryHash     string
	Flags         []string
	Sample        *event.Event
	Histogram     []int // 8 log10 buckets: <1ms, 1-10ms, 10-100ms, 100ms-1s, 1-10s, 10-100s, 100-1000s, 1000s+
}

// Aggregator is concurrency-safe aggregator keyed by class ID.
type Aggregator struct {
	mu             sync.Mutex
	classes        map[string]*Class
	reservoirSize  int
	totalEvents    int64
	totalMs        float64
	earliest       time.Time
	latest         time.Time
}

func New() *Aggregator {
	return &Aggregator{
		classes:       make(map[string]*Class),
		reservoirSize: DefaultReservoirSize,
	}
}

func (a *Aggregator) TotalEvents() int64 { return a.totalEvents }
func (a *Aggregator) TotalMs() float64   { return a.totalMs }
func (a *Aggregator) Earliest() time.Time { return a.earliest }
func (a *Aggregator) Latest() time.Time   { return a.latest }

func (a *Aggregator) Add(ev *event.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c, ok := a.classes[ev.ClassID]
	if !ok {
		c = &Class{
			ID:            ev.ClassID,
			Op:            ev.Op,
			Namespace:     ev.Namespace,
			Shape:         ev.Shape,
			MinMs:         math.Inf(1),
			PlanSummaries: make(map[string]int64),
			PlanCacheKeys: make(map[string]struct{}),
			QueryHashes:   make(map[string]struct{}),
			AppNames:      make(map[string]int64),
			Users:         make(map[string]int64),
			Namespaces:    make(map[string]int64),
			FirstSeen:     ev.Timestamp,
			rng:           rand.New(rand.NewSource(hashString(ev.ClassID))),
		}
		a.classes[ev.ClassID] = c
	}

	c.Count++
	dur := float64(ev.DurationMs)
	c.SumMs += dur
	c.SumSqMs += dur * dur
	if dur < c.MinMs {
		c.MinMs = dur
	}
	if dur > c.MaxMs {
		c.MaxMs = dur
	}
	c.SumDocsExam += ev.DocsExamined
	c.SumKeysExam += ev.KeysExamined
	c.SumDocsRet += ev.DocsReturned
	c.SumYields += ev.NumYields
	c.SumRespLen += ev.ResponseLength
	if ev.PlanSummary != "" {
		c.PlanSummaries[ev.PlanSummary]++
	}
	if ev.PlanCacheKey != "" {
		c.PlanCacheKeys[ev.PlanCacheKey] = struct{}{}
	}
	if ev.QueryHash != "" {
		c.QueryHashes[ev.QueryHash] = struct{}{}
	}
	if ev.AppName != "" {
		c.AppNames[ev.AppName]++
	}
	if ev.User != "" {
		c.Users[ev.User]++
	}
	if ev.Namespace != "" {
		c.Namespaces[ev.Namespace]++
	}
	if !ev.Timestamp.IsZero() {
		if c.FirstSeen.IsZero() || ev.Timestamp.Before(c.FirstSeen) {
			c.FirstSeen = ev.Timestamp
		}
		if ev.Timestamp.After(c.LastSeen) {
			c.LastSeen = ev.Timestamp
		}
	}

	// Sample: keep the slowest query seen for this class.
	if c.Sample == nil || ev.DurationMs > c.Sample.DurationMs {
		c.Sample = ev
	}

	// Reservoir sampling for percentile estimation.
	c.seen++
	if len(c.durationSamples) < a.reservoirSize {
		c.durationSamples = append(c.durationSamples, dur)
	} else {
		idx := c.rng.Int63n(c.seen)
		if int(idx) < a.reservoirSize {
			c.durationSamples[idx] = dur
		}
	}

	// Corpus-level counters.
	a.totalEvents++
	a.totalMs += dur
	if !ev.Timestamp.IsZero() {
		if a.earliest.IsZero() || ev.Timestamp.Before(a.earliest) {
			a.earliest = ev.Timestamp
		}
		if ev.Timestamp.After(a.latest) {
			a.latest = ev.Timestamp
		}
	}
}

// Summaries returns per-class computed summaries. Safe to call while Add is
// running; takes the lock.
func (a *Aggregator) Summaries() []*Summary {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]*Summary, 0, len(a.classes))
	for _, c := range a.classes {
		out = append(out, summarize(c))
	}
	return out
}

func summarize(c *Class) *Summary {
	s := &Summary{
		ID:            c.ID,
		Op:            c.Op,
		Namespace:     c.Namespace,
		Shape:         c.Shape,
		Count:         c.Count,
		FirstSeen:     c.FirstSeen,
		LastSeen:      c.LastSeen,
		TotalMs:       c.SumMs,
		MinMs:         c.MinMs,
		MaxMs:         c.MaxMs,
		DistinctPlans: len(c.PlanCacheKeys),
		Sample:        c.Sample,
	}
	if math.IsInf(s.MinMs, 1) {
		s.MinMs = 0
	}
	if c.Count > 0 {
		s.AvgMs = c.SumMs / float64(c.Count)
		s.AvgDocsExam = float64(c.SumDocsExam) / float64(c.Count)
		s.AvgDocsRet = float64(c.SumDocsRet) / float64(c.Count)
		s.AvgKeysExam = float64(c.SumKeysExam) / float64(c.Count)
		ret := float64(c.SumDocsRet)
		if ret > 0 {
			s.ExamReturnRatio = float64(c.SumDocsExam) / ret
		} else if c.SumDocsExam > 0 {
			// Sentinel for "examined but none returned"; report layers treat
			// values past 1e18 as infinite. Keeps it JSON-marshalable.
			s.ExamReturnRatio = 1e18
		}
		if c.Count > 1 {
			mean := s.AvgMs
			variance := c.SumSqMs/float64(c.Count) - mean*mean
			if variance < 0 {
				variance = 0
			}
			s.StddevMs = math.Sqrt(variance)
		}
	}
	if len(c.durationSamples) > 0 {
		sorted := append([]float64(nil), c.durationSamples...)
		sort.Float64s(sorted)
		s.MedianMs = percentile(sorted, 0.50)
		s.P95Ms = percentile(sorted, 0.95)
		s.P99Ms = percentile(sorted, 0.99)
		s.Histogram = buildHistogram(sorted)
	}
	s.PlanSummary = dominant(c.PlanSummaries)
	if len(c.QueryHashes) == 1 {
		for k := range c.QueryHashes {
			s.QueryHash = k
		}
	}
	return s
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := p * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo] + (sorted[hi]-sorted[lo])*frac
}

func dominant(m map[string]int64) string {
	var best string
	var bestN int64
	for k, v := range m {
		if v > bestN {
			best = k
			bestN = v
		}
	}
	return best
}

func buildHistogram(samples []float64) []int {
	h := make([]int, 8)
	for _, ms := range samples {
		var idx int
		switch {
		case ms < 1:
			idx = 0
		case ms < 10:
			idx = 1
		case ms < 100:
			idx = 2
		case ms < 1000:
			idx = 3
		case ms < 10_000:
			idx = 4
		case ms < 100_000:
			idx = 5
		case ms < 1_000_000:
			idx = 6
		default:
			idx = 7
		}
		h[idx]++
	}
	return h
}

func hashString(s string) int64 {
	var h int64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= int64(s[i])
		h *= 1099511628211
	}
	if h < 0 {
		h = -h
	}
	return h
}
