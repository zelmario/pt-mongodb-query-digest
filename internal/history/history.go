// Package history persists analysis runs to a local SQLite database so they
// can be listed, re-viewed, and diffed later. Uses modernc.org/sqlite — pure
// Go, no cgo, no runtime dependency beyond the embedded driver.
package history

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/report"
)

// Store is the local SQLite-backed history database.
type Store struct {
	db *sql.DB
}

// DefaultPath is where we stash runs by default. Override via --history-db.
func DefaultPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		if home, err2 := os.UserHomeDir(); err2 == nil {
			base = filepath.Join(home, ".local", "share")
		} else {
			return "", err
		}
	}
	dir := filepath.Join(base, "pt-mongodb-query-digest")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(dir, "history.db"), nil
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

const schema = `
CREATE TABLE IF NOT EXISTS runs (
	name         TEXT PRIMARY KEY,
	saved_at     TIMESTAMP NOT NULL,
	source       TEXT NOT NULL,
	total_events INTEGER NOT NULL,
	total_ms     REAL NOT NULL,
	earliest     TIMESTAMP,
	latest       TIMESTAMP
);
CREATE TABLE IF NOT EXISTS classes (
	run_name          TEXT NOT NULL REFERENCES runs(name) ON DELETE CASCADE,
	id                TEXT NOT NULL,
	op                TEXT NOT NULL,
	namespace         TEXT NOT NULL,
	shape             TEXT NOT NULL,
	count             INTEGER NOT NULL,
	total_ms          REAL NOT NULL,
	min_ms            REAL NOT NULL,
	max_ms            REAL NOT NULL,
	avg_ms            REAL NOT NULL,
	median_ms         REAL NOT NULL,
	p95_ms            REAL NOT NULL,
	p99_ms            REAL NOT NULL,
	stddev_ms         REAL NOT NULL,
	avg_docs_exam     REAL NOT NULL,
	avg_docs_ret      REAL NOT NULL,
	avg_keys_exam     REAL NOT NULL,
	exam_return_ratio REAL NOT NULL,
	plan_summary      TEXT,
	distinct_plans    INTEGER,
	query_hash        TEXT,
	flags             TEXT,
	first_seen        TIMESTAMP,
	last_seen         TIMESTAMP,
	PRIMARY KEY (run_name, id)
);
CREATE INDEX IF NOT EXISTS idx_classes_run ON classes(run_name);
`

// RunInfo is the listing/summary of a saved run.
type RunInfo struct {
	Name        string
	SavedAt     time.Time
	Source      string
	TotalEvents int64
	TotalMs     float64
	Earliest    *time.Time
	Latest      *time.Time
	ClassCount  int
}

// Save replaces any existing run with the same name.
func (s *Store) Save(name string, ctx report.Context, sums []*aggregator.Summary) error {
	if name == "" {
		return errors.New("name is required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM runs WHERE name = ?`, name); err != nil {
		return err
	}

	if _, err := tx.Exec(
		`INSERT INTO runs(name, saved_at, source, total_events, total_ms, earliest, latest)
		 VALUES(?,?,?,?,?,?,?)`,
		name, time.Now().UTC(), ctx.Source, ctx.TotalEvents, ctx.TotalMs,
		nullableTime(ctx.Earliest), nullableTime(ctx.Latest),
	); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO classes(
		run_name, id, op, namespace, shape, count, total_ms, min_ms, max_ms,
		avg_ms, median_ms, p95_ms, p99_ms, stddev_ms,
		avg_docs_exam, avg_docs_ret, avg_keys_exam, exam_return_ratio,
		plan_summary, distinct_plans, query_hash, flags, first_seen, last_seen
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range sums {
		if _, err := stmt.Exec(
			name, c.ID, c.Op, c.Namespace, c.Shape, c.Count, c.TotalMs,
			c.MinMs, c.MaxMs, c.AvgMs, c.MedianMs, c.P95Ms, c.P99Ms, c.StddevMs,
			c.AvgDocsExam, c.AvgDocsRet, c.AvgKeysExam, c.ExamReturnRatio,
			c.PlanSummary, c.DistinctPlans, c.QueryHash,
			strings.Join(c.Flags, ","),
			nullableTime(c.FirstSeen), nullableTime(c.LastSeen),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) List() ([]RunInfo, error) {
	rows, err := s.db.Query(`
		SELECT r.name, r.saved_at, r.source, r.total_events, r.total_ms,
		       r.earliest, r.latest, COUNT(c.id)
		FROM runs r LEFT JOIN classes c ON c.run_name = r.name
		GROUP BY r.name ORDER BY r.saved_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RunInfo
	for rows.Next() {
		var r RunInfo
		var earliest, latest sql.NullTime
		if err := rows.Scan(&r.Name, &r.SavedAt, &r.Source, &r.TotalEvents,
			&r.TotalMs, &earliest, &latest, &r.ClassCount); err != nil {
			return nil, err
		}
		if earliest.Valid {
			t := earliest.Time
			r.Earliest = &t
		}
		if latest.Valid {
			t := latest.Time
			r.Latest = &t
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Delete(name string) error {
	res, err := s.db.Exec(`DELETE FROM runs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no run named %q", name)
	}
	return nil
}

// Load returns the report context plus class summaries for a saved run.
func (s *Store) Load(name string) (report.Context, []*aggregator.Summary, error) {
	var rctx report.Context
	var savedAt time.Time
	var earliest, latest sql.NullTime
	err := s.db.QueryRow(
		`SELECT saved_at, source, total_events, total_ms, earliest, latest
		 FROM runs WHERE name = ?`, name).
		Scan(&savedAt, &rctx.Source, &rctx.TotalEvents, &rctx.TotalMs, &earliest, &latest)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rctx, nil, fmt.Errorf("no run named %q", name)
		}
		return rctx, nil, err
	}
	rctx.StartedAt = savedAt
	if earliest.Valid {
		rctx.Earliest = earliest.Time
	}
	if latest.Valid {
		rctx.Latest = latest.Time
	}

	rows, err := s.db.Query(`
		SELECT id, op, namespace, shape, count, total_ms, min_ms, max_ms,
		       avg_ms, median_ms, p95_ms, p99_ms, stddev_ms,
		       avg_docs_exam, avg_docs_ret, avg_keys_exam, exam_return_ratio,
		       plan_summary, distinct_plans, query_hash, flags,
		       first_seen, last_seen
		FROM classes WHERE run_name = ?`, name)
	if err != nil {
		return rctx, nil, err
	}
	defer rows.Close()

	var sums []*aggregator.Summary
	for rows.Next() {
		s := &aggregator.Summary{}
		var planSum, qhash, flagsCSV sql.NullString
		var distinctPlans sql.NullInt64
		var firstSeen, lastSeen sql.NullTime
		if err := rows.Scan(&s.ID, &s.Op, &s.Namespace, &s.Shape, &s.Count,
			&s.TotalMs, &s.MinMs, &s.MaxMs, &s.AvgMs, &s.MedianMs,
			&s.P95Ms, &s.P99Ms, &s.StddevMs, &s.AvgDocsExam, &s.AvgDocsRet,
			&s.AvgKeysExam, &s.ExamReturnRatio,
			&planSum, &distinctPlans, &qhash, &flagsCSV,
			&firstSeen, &lastSeen); err != nil {
			return rctx, nil, err
		}
		s.PlanSummary = planSum.String
		s.QueryHash = qhash.String
		s.DistinctPlans = int(distinctPlans.Int64)
		if flagsCSV.String != "" {
			s.Flags = strings.Split(flagsCSV.String, ",")
		}
		if firstSeen.Valid {
			s.FirstSeen = firstSeen.Time
		}
		if lastSeen.Valid {
			s.LastSeen = lastSeen.Time
		}
		sums = append(sums, s)
	}
	return rctx, sums, rows.Err()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}
