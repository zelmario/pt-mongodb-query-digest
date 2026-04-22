package parser

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
)

const slowQueryMarker = `"msg":"Slow query"`

// OpenLog opens a log source. Path of "-" reads stdin. .gz is decompressed
// transparently. Caller must close the returned ReadCloser.
func OpenLog(path string) (io.ReadCloser, error) {
	if path == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		return &gzipReadCloser{gz: gz, f: f}, nil
	}
	return f, nil
}

type gzipReadCloser struct {
	gz *gzip.Reader
	f  *os.File
}

func (g *gzipReadCloser) Read(p []byte) (int, error) { return g.gz.Read(p) }
func (g *gzipReadCloser) Close() error {
	gerr := g.gz.Close()
	ferr := g.f.Close()
	if gerr != nil {
		return gerr
	}
	return ferr
}

// ParseLog reads JSON log lines from r and emits Events for slow-query entries.
// Unparseable or unrelated lines are skipped silently; parse errors are
// reported via the returned error only when r itself fails.
func ParseLog(ctx context.Context, r io.Reader, out chan<- *event.Event) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 32*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Bytes()
		if !bytes.Contains(line, []byte(slowQueryMarker)) {
			continue
		}
		ev, err := parseLogLine(line)
		if err != nil || ev == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- ev:
		}
	}
	return scanner.Err()
}

type logLine struct {
	T    map[string]any `json:"t"`
	S    string         `json:"s"`
	C    string         `json:"c"`
	Msg  string         `json:"msg"`
	Attr map[string]any `json:"attr"`
}

func parseLogLine(raw []byte) (*event.Event, error) {
	var ll logLine
	if err := json.Unmarshal(raw, &ll); err != nil {
		return nil, err
	}
	if ll.Msg != "Slow query" || ll.Attr == nil {
		return nil, nil
	}
	ev := &event.Event{Timestamp: extractTimestamp(ll.T)}
	ev.Namespace, _ = ll.Attr["ns"].(string)
	ev.Database, ev.Collection = event.SplitNS(ev.Namespace)
	ev.AppName, _ = ll.Attr["appName"].(string)
	ev.Client, _ = ll.Attr["remote"].(string)
	ev.PlanSummary = extractPlanSummary(ll.Attr["planSummary"])
	ev.QueryHash, _ = ll.Attr["queryHash"].(string)
	ev.PlanCacheKey, _ = ll.Attr["planCacheKey"].(string)
	ev.DurationMs = readInt(ll.Attr["durationMillis"])
	ev.DocsExamined = readInt(ll.Attr["docsExamined"])
	ev.KeysExamined = readInt(ll.Attr["keysExamined"])
	ev.DocsReturned = readInt(ll.Attr["nreturned"])
	ev.NumYields = readInt(ll.Attr["numYields"])
	ev.ResponseLength = readInt(ll.Attr["reslen"])
	ev.User = extractUser(ll.Attr)

	cmd, ok := ll.Attr["command"].(map[string]any)
	if !ok {
		// Older / alternative formats may stash the command under "q", "u"
		// at the attr level, or omit it entirely. Skip if there's no shape.
		return nil, nil
	}
	ev.Command = cmd
	return ev, nil
}

func extractTimestamp(t map[string]any) time.Time {
	if t == nil {
		return time.Time{}
	}
	if v, ok := t["$date"]; ok {
		if s, ok := v.(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return ts
			}
		}
	}
	return time.Time{}
}

func extractPlanSummary(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []any:
		parts := make([]string, 0, len(val))
		for _, e := range val {
			if s, ok := e.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, ",")
	}
	return ""
}

func extractUser(attr map[string]any) string {
	if u, ok := attr["user"].(string); ok && u != "" {
		return u
	}
	if arr, ok := attr["effectiveUsers"].([]any); ok && len(arr) > 0 {
		if m, ok := arr[0].(map[string]any); ok {
			u, _ := m["user"].(string)
			db, _ := m["db"].(string)
			if u != "" && db != "" {
				return fmt.Sprintf("%s@%s", u, db)
			}
			return u
		}
	}
	return ""
}

func readInt(v any) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return val
	case map[string]any:
		if s, ok := val["$numberLong"].(string); ok {
			var n int64
			fmt.Sscanf(s, "%d", &n)
			return n
		}
		if f, ok := val["$numberInt"].(string); ok {
			var n int64
			fmt.Sscanf(f, "%d", &n)
			return n
		}
	}
	return 0
}
