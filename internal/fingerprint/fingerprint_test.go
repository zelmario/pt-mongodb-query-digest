package fingerprint

import "testing"

func TestCanonicalize_FindGroupsLiterals(t *testing.T) {
	a := map[string]any{
		"find":   "orders",
		"filter": map[string]any{"status": "pending", "userId": 42.0},
		"limit":  20.0,
		"sort":   map[string]any{"createdAt": -1.0},
		"$db":    "mydb",
	}
	b := map[string]any{
		"find":   "orders",
		"filter": map[string]any{"status": "shipped", "userId": 7.0},
		"limit":  100.0,
		"sort":   map[string]any{"createdAt": -1.0},
		"$db":    "mydb",
	}
	op1, coll1, s1 := Canonicalize(a)
	op2, coll2, s2 := Canonicalize(b)
	if op1 != "find" || op2 != "find" {
		t.Fatalf("op mismatch: %q %q", op1, op2)
	}
	if coll1 != "orders" || coll2 != "orders" {
		t.Fatalf("coll mismatch: %q %q", coll1, coll2)
	}
	if s1 != s2 {
		t.Fatalf("expected identical shapes; got\n  %s\n  %s", s1, s2)
	}
}

func TestCanonicalize_InArrayCollapses(t *testing.T) {
	small := map[string]any{
		"find":   "orders",
		"filter": map[string]any{"status": map[string]any{"$in": []any{"a", "b"}}},
	}
	large := map[string]any{
		"find":   "orders",
		"filter": map[string]any{"status": map[string]any{"$in": []any{"a", "b", "c", "d", "e"}}},
	}
	_, _, sSmall := Canonicalize(small)
	_, _, sLarge := Canonicalize(large)
	if sSmall != sLarge {
		t.Fatalf("$in cardinality leaked into shape:\n  %s\n  %s", sSmall, sLarge)
	}
}

func TestCanonicalize_SortPreserved(t *testing.T) {
	asc := map[string]any{"find": "x", "sort": map[string]any{"a": 1.0}}
	desc := map[string]any{"find": "x", "sort": map[string]any{"a": -1.0}}
	_, _, s1 := Canonicalize(asc)
	_, _, s2 := Canonicalize(desc)
	if s1 == s2 {
		t.Fatalf("sort direction collapsed: %s", s1)
	}
}

func TestCanonicalize_OperatorsPreserved(t *testing.T) {
	q1 := map[string]any{"find": "x", "filter": map[string]any{"n": map[string]any{"$gt": 5.0}}}
	q2 := map[string]any{"find": "x", "filter": map[string]any{"n": map[string]any{"$lt": 5.0}}}
	_, _, s1 := Canonicalize(q1)
	_, _, s2 := Canonicalize(q2)
	if s1 == s2 {
		t.Fatalf("different operators collapsed: %s", s1)
	}
}

func TestCanonicalize_ExtJSONDateCollapses(t *testing.T) {
	q1 := map[string]any{
		"find":   "x",
		"filter": map[string]any{"ts": map[string]any{"$date": "2024-01-01T00:00:00Z"}},
	}
	q2 := map[string]any{
		"find":   "x",
		"filter": map[string]any{"ts": map[string]any{"$date": "2024-06-01T00:00:00Z"}},
	}
	_, _, s1 := Canonicalize(q1)
	_, _, s2 := Canonicalize(q2)
	if s1 != s2 {
		t.Fatalf("$date values leaked into shape:\n  %s\n  %s", s1, s2)
	}
}

func TestCanonicalize_AggregatePipeline(t *testing.T) {
	p1 := map[string]any{
		"aggregate": "orders",
		"pipeline": []any{
			map[string]any{"$match": map[string]any{"user": 1.0}},
			map[string]any{"$group": map[string]any{"_id": "$status", "n": map[string]any{"$sum": 1.0}}},
		},
	}
	p2 := map[string]any{
		"aggregate": "orders",
		"pipeline": []any{
			map[string]any{"$match": map[string]any{"user": 99.0}},
			map[string]any{"$group": map[string]any{"_id": "$status", "n": map[string]any{"$sum": 1.0}}},
		},
	}
	op, _, s1 := Canonicalize(p1)
	_, _, s2 := Canonicalize(p2)
	if op != "aggregate" {
		t.Fatalf("op=%q", op)
	}
	if s1 != s2 {
		t.Fatalf("pipeline literals leaked:\n  %s\n  %s", s1, s2)
	}
}

func TestClassID_Stable(t *testing.T) {
	id1 := ClassID("find", "db.coll", `{"filter":{"x":"?"}}`)
	id2 := ClassID("find", "db.coll", `{"filter":{"x":"?"}}`)
	if id1 != id2 {
		t.Fatalf("class id not stable: %s vs %s", id1, id2)
	}
	id3 := ClassID("find", "db.other", `{"filter":{"x":"?"}}`)
	if id1 == id3 {
		t.Fatalf("class id should differ for different ns")
	}
}
