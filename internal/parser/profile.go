package parser

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/pt-mongodb-query-digest/internal/event"
)

// ProfileOptions controls a read of system.profile.
type ProfileOptions struct {
	URI      string
	Database string // empty means "read profiler from every db visible to the user"
	Since    bson.DateTime
	Until    bson.DateTime
}

// ParseProfile connects to MongoDB, reads matching system.profile documents,
// and emits Events to out. Caller owns out; this function closes it.
func ParseProfile(ctx context.Context, opts ProfileOptions, out chan<- *event.Event) error {
	defer close(out)

	clientOpts := options.Client().ApplyURI(opts.URI)
	if err := clientOpts.Validate(); err != nil {
		return fmt.Errorf("invalid MongoDB URI: %w", err)
	}
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	dbNames, err := resolveDatabases(ctx, client, opts)
	if err != nil {
		return err
	}
	if len(dbNames) == 0 {
		return fmt.Errorf("no database to read profiler from (pass --db or include it in the URI path)")
	}

	filter := bson.M{}
	if opts.Since != 0 || opts.Until != 0 {
		tsFilter := bson.M{}
		if opts.Since != 0 {
			tsFilter["$gte"] = opts.Since
		}
		if opts.Until != 0 {
			tsFilter["$lt"] = opts.Until
		}
		filter["ts"] = tsFilter
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "ts", Value: 1}})

	for _, dbName := range dbNames {
		coll := client.Database(dbName).Collection("system.profile")
		cur, err := coll.Find(ctx, filter, findOpts)
		if err != nil {
			return fmt.Errorf("find %s.system.profile: %w", dbName, err)
		}
		if err := drainCursor(ctx, cur, out); err != nil {
			_ = cur.Close(context.Background())
			return fmt.Errorf("read %s.system.profile: %w", dbName, err)
		}
		_ = cur.Close(context.Background())
	}
	return nil
}

func resolveDatabases(ctx context.Context, client *mongo.Client, opts ProfileOptions) ([]string, error) {
	if opts.Database != "" {
		return []string{opts.Database}, nil
	}
	names, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	out := names[:0]
	for _, n := range names {
		switch n {
		case "admin", "local", "config":
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

func drainCursor(ctx context.Context, cur *mongo.Cursor, out chan<- *event.Event) error {
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		ev := profileDocToEvent(doc)
		if ev == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- ev:
		}
	}
	return cur.Err()
}

func profileDocToEvent(doc bson.M) *event.Event {
	ev := &event.Event{}
	ev.Namespace, _ = doc["ns"].(string)
	ev.Database, ev.Collection = event.SplitNS(ev.Namespace)
	ev.AppName, _ = doc["appName"].(string)
	ev.Client, _ = doc["client"].(string)
	ev.QueryHash, _ = doc["queryHash"].(string)
	ev.PlanCacheKey, _ = doc["planCacheKey"].(string)
	ev.PlanSummary = extractPlanSummary(doc["planSummary"])
	ev.DurationMs = readBSONInt(doc["millis"])
	ev.DocsExamined = readBSONInt(doc["docsExamined"])
	ev.KeysExamined = readBSONInt(doc["keysExamined"])
	ev.DocsReturned = readBSONInt(doc["nreturned"])
	ev.NumYields = readBSONInt(doc["numYield"])
	if ev.NumYields == 0 {
		ev.NumYields = readBSONInt(doc["numYields"])
	}
	ev.ResponseLength = readBSONInt(doc["responseLength"])
	ev.User = profileUser(doc)
	if ts, ok := doc["ts"].(bson.DateTime); ok {
		ev.Timestamp = ts.Time()
	}

	cmd := extractProfileCommand(doc)
	if cmd == nil {
		return nil
	}
	ev.Command = cmd
	return ev
}

// extractProfileCommand pulls the command doc out of a profile record. Older
// profiler records used "query" + top-level op; newer ones stash everything
// under "command". Normalize both.
func extractProfileCommand(doc bson.M) map[string]any {
	if raw, ok := doc["command"]; ok {
		if m := toMap(raw); m != nil {
			return m
		}
	}
	op, _ := doc["op"].(string)
	query := toMap(doc["query"])
	if query == nil {
		return nil
	}
	// Reconstruct a command-shaped doc so downstream sees a uniform structure.
	_, coll := event.SplitNS(stringOrEmpty(doc["ns"]))
	out := map[string]any{}
	for k, v := range query {
		out[k] = v
	}
	switch strings.ToLower(op) {
	case "query", "":
		if _, hasFilter := out["filter"]; !hasFilter {
			if q, ok := out["$query"]; ok {
				out["filter"] = q
				delete(out, "$query")
			}
		}
		out["find"] = coll
	case "update":
		out["update"] = coll
	case "remove":
		out["delete"] = coll
	case "insert":
		out["insert"] = coll
	case "getmore":
		out["getMore"] = int64(0)
	case "command":
		// already command-shaped
	}
	return out
}

func toMap(v any) map[string]any {
	switch val := v.(type) {
	case bson.M:
		return map[string]any(val)
	case map[string]any:
		return val
	case bson.D:
		out := make(map[string]any, len(val))
		for _, e := range val {
			out[e.Key] = e.Value
		}
		return out
	}
	return nil
}

func readBSONInt(v any) int64 {
	switch val := v.(type) {
	case int32:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	}
	return 0
}

func profileUser(doc bson.M) string {
	if u, ok := doc["user"].(string); ok && u != "" {
		return u
	}
	if arr, ok := doc["allUsers"].(bson.A); ok && len(arr) > 0 {
		if m, ok := arr[0].(bson.M); ok {
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

func stringOrEmpty(v any) string {
	s, _ := v.(string)
	return s
}
