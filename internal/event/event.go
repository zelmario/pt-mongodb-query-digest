package event

import (
	"strings"
	"time"
)

type Event struct {
	Timestamp      time.Time
	Op             string
	Namespace      string
	Database       string
	Collection     string
	DurationMs     int64
	DocsExamined   int64
	KeysExamined   int64
	DocsReturned   int64
	NumYields      int64
	ResponseLength int64
	PlanSummary    string
	QueryHash      string
	PlanCacheKey   string
	AppName        string
	User           string
	Client         string
	Command        map[string]any
	Shape          string
	ClassID        string
}

func SplitNS(ns string) (db, coll string) {
	i := strings.IndexByte(ns, '.')
	if i < 0 {
		return ns, ""
	}
	return ns[:i], ns[i+1:]
}
