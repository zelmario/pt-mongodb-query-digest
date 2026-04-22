package fingerprint

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Canonicalize walks a MongoDB command and produces a stable shape string,
// the op name, and the target collection. Shape strings are sorted-key JSON
// with data-bearing leaves replaced by "?".
func Canonicalize(cmd map[string]any) (op, coll, shape string) {
	if cmd == nil {
		return "", "", "{}"
	}
	op, coll = identifyOp(cmd)
	normalized := normalizeCommand(cmd, op)
	shape = canonJSON(normalized)
	return op, coll, shape
}

// ClassID returns a short, stable hex id for a (op, ns, shape) triple.
func ClassID(op, ns, shape string) string {
	h := sha1.New()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write([]byte(ns))
	h.Write([]byte{0})
	h.Write([]byte(shape))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Known op names in priority order. Map iteration in Go is randomized, so we
// check this list against the command rather than picking the first key.
var knownOps = []string{
	"find", "aggregate", "update", "delete", "insert",
	"findAndModify", "findandmodify",
	"count", "distinct",
	"getMore", "getmore",
}

// Wrappers that represent scalar values in MongoDB Extended JSON. Collapsed
// to "?" during fingerprinting. $regex is deliberately NOT here because in
// query context it is an operator, not a wrapper.
var extJSONWrappers = map[string]bool{
	"$date": true, "$oid": true, "$numberLong": true, "$numberInt": true,
	"$numberDouble": true, "$numberDecimal": true, "$binary": true,
	"$code": true, "$uuid": true, "$timestamp": true, "$symbol": true,
	"$minKey": true, "$maxKey": true, "$undefined": true,
	"$regularExpression": true,
}

// Top-level command fields that are request metadata, not shape.
var skipFields = map[string]bool{
	"$db": true, "lsid": true, "$clusterTime": true, "txnNumber": true,
	"$readPreference": true, "$audit": true, "comment": true,
	"ordered": true, "batchSize": true, "maxTimeMS": true,
	"autocommit": true, "apiVersion": true, "apiStrict": true,
	"apiDeprecationErrors": true, "readConcern": true, "writeConcern": true,
}

func identifyOp(cmd map[string]any) (op, coll string) {
	for _, name := range knownOps {
		if v, ok := cmd[name]; ok {
			op = normalizeOp(name)
			if s, ok := v.(string); ok {
				coll = s
			}
			return op, coll
		}
	}
	return "", ""
}

func normalizeOp(name string) string {
	switch strings.ToLower(name) {
	case "findandmodify":
		return "findAndModify"
	case "getmore":
		return "getMore"
	default:
		return name
	}
}

func normalizeCommand(cmd map[string]any, op string) map[string]any {
	out := make(map[string]any)
	for k, v := range cmd {
		if skipFields[k] {
			continue
		}
		if strings.EqualFold(k, op) {
			// The op key itself holds the collection name; already captured.
			continue
		}
		switch k {
		case "filter", "query", "q":
			out["filter"] = normalizeQuery(v)
		case "update", "u":
			out["update"] = normalizeQuery(v)
		case "pipeline":
			out["pipeline"] = normalizePipeline(v)
		case "sort", "projection", "fields", "hint":
			// Preserve structure but not numeric literals in filter-like
			// positions. Sort directions (1/-1) and projection flags (0/1)
			// are semantic, so we leave them alone.
			out[k] = preserveStructure(v)
		case "limit", "skip":
			out[k] = "?"
		case "updates", "deletes", "documents":
			// Batch operations: each entry is a sub-op; recurse.
			out[k] = normalizeArray(v, normalizeQuery)
		default:
			out[k] = normalizeQuery(v)
		}
	}
	return out
}

// normalizeQuery replaces scalar values with "?" while preserving operator
// structure. Extended-JSON wrappers collapse to "?".
func normalizeQuery(v any) any {
	switch val := v.(type) {
	case map[string]any:
		if len(val) == 1 {
			for k := range val {
				if extJSONWrappers[k] {
					return "?"
				}
			}
		}
		out := make(map[string]any, len(val))
		for k, sub := range val {
			if k == "$in" || k == "$nin" {
				out[k] = []any{"?"}
				continue
			}
			out[k] = normalizeQuery(sub)
		}
		return out
	case []any:
		if allScalar(val) {
			if len(val) == 0 {
				return []any{}
			}
			return []any{"?"}
		}
		out := make([]any, len(val))
		for i, e := range val {
			out[i] = normalizeQuery(e)
		}
		return out
	case nil:
		return "?"
	default:
		return "?"
	}
}

// preserveStructure keeps the literal values (for sort directions, hint shape,
// etc.) but still collapses extJSON wrappers.
func preserveStructure(v any) any {
	switch val := v.(type) {
	case map[string]any:
		if len(val) == 1 {
			for k := range val {
				if extJSONWrappers[k] {
					return "?"
				}
			}
		}
		out := make(map[string]any, len(val))
		for k, sub := range val {
			out[k] = preserveStructure(sub)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, e := range val {
			out[i] = preserveStructure(e)
		}
		return out
	default:
		return val
	}
}

func normalizePipeline(v any) any {
	arr, ok := v.([]any)
	if !ok {
		return normalizeQuery(v)
	}
	out := make([]any, len(arr))
	for i, stage := range arr {
		out[i] = normalizeQuery(stage)
	}
	return out
}

func normalizeArray(v any, fn func(any) any) any {
	arr, ok := v.([]any)
	if !ok {
		return fn(v)
	}
	out := make([]any, len(arr))
	for i, e := range arr {
		out[i] = fn(e)
	}
	return out
}

func allScalar(arr []any) bool {
	for _, e := range arr {
		switch e.(type) {
		case map[string]any, []any:
			return false
		}
	}
	return true
}

// canonJSON writes sorted-key JSON without any external encoder; keeps output
// deterministic and avoids pulling in reflection machinery.
func canonJSON(v any) string {
	var sb strings.Builder
	encode(&sb, v)
	return sb.String()
}

func encode(sb *strings.Builder, v any) {
	switch val := v.(type) {
	case map[string]any:
		sb.WriteByte('{')
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeString(sb, k)
			sb.WriteByte(':')
			encode(sb, val[k])
		}
		sb.WriteByte('}')
	case []any:
		sb.WriteByte('[')
		for i, e := range val {
			if i > 0 {
				sb.WriteByte(',')
			}
			encode(sb, e)
		}
		sb.WriteByte(']')
	case string:
		writeString(sb, val)
	case bool:
		if val {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case nil:
		sb.WriteString("null")
	case float64:
		fmt.Fprintf(sb, "%v", val)
	case int, int32, int64, uint, uint32, uint64:
		fmt.Fprintf(sb, "%d", val)
	default:
		writeString(sb, fmt.Sprintf("%v", val))
	}
}

func writeString(sb *strings.Builder, s string) {
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(sb, `\u%04x`, r)
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
}
