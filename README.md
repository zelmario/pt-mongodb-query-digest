# pt-mongodb-query-digest

Find the slow MongoDB queries, group them by shape, and surface the worst offenders — with remediation hints, a local web UI, and offline history. Single static Go binary, no external dependencies.

Built for operators and backend engineers who want a quick local answer to "why is the database slow right now" without standing up Atlas, Ops Manager, or a dashboard stack.

> The binary name is long; most users alias it: `alias mqd=pt-mongodb-query-digest`.

## Features

- Two inputs, user's choice: **`system.profile` collection** or **mongod JSON log**.
- **Local web UI** (`--web`) with sort/filter/drill-down and per-class histograms; or **offline HTML export** (`--html FILE`) for ticket handoff.
- **Shape-based fingerprinting** — groups `{x:1}` and `{x:42}` into the same class; respects MongoDB's `queryHash` when present.
- **Anti-pattern detector** — flags COLLSCAN, unanchored `$regex`, large `$in`, `$ne`/`$nin`, `$where`, unbounded `skip()`, and plan flips, each with a one-line fix hint.
- **Diff mode** — compare two saved runs; reports regressions, improvements, new classes, and gone classes.
- **Local history in SQLite** (pure Go, no cgo) — no external database required for longitudinal tracking.
- Output formats: terminal text, JSON.
- Read-only. Never modifies the profiler or anything else on the target server.

## Install

From source (Go 1.22+; the toolchain will auto-upgrade for one of the indirect deps):

```sh
git clone <this repo>
cd pt-mongodb-query-digest
go build -trimpath -ldflags="-s -w" -o pt-mongodb-query-digest .
sudo install pt-mongodb-query-digest /usr/local/bin/    # optional
```

The resulting binary is ~17 MB, statically linked against the Go runtime plus `libc`. No other runtime dependencies.

## Quickstart

```sh
# Analyze the profiler on a running mongod (static text report)
pt-mongodb-query-digest profile 'mongodb://localhost:27017/mydb'

# Analyze a mongod JSON log file
pt-mongodb-query-digest log /var/log/mongodb/mongod.log

# Launch local web UI and open in browser
pt-mongodb-query-digest log mongod.log --web

# Export a self-contained HTML report for a ticket
pt-mongodb-query-digest log mongod.log --html report.html
```

## Inputs

### Profiler

Enable the profiler on the database(s) you care about:

```js
// in mongosh
db.setProfilingLevel(1, { slowms: 100 })
```

Then:

```sh
pt-mongodb-query-digest profile 'mongodb://user:pass@host/mydb?replicaSet=rs0'
```

The tool reads `system.profile` in timestamp order, honoring `--since` / `--until` server-side via a `ts` filter. It never writes to the target database.

### Log file

Structured JSON logs (MongoDB 4.4+):

```sh
pt-mongodb-query-digest log mongod.log
pt-mongodb-query-digest log mongod.log.gz       # .gz auto-detected
pt-mongodb-query-digest log -                   # read from stdin
cat mongod-*.log | pt-mongodb-query-digest log -
```

Only slow-query entries are parsed; everything else is ignored. Text-format logs (pre-4.4) are not supported.

## Common examples

```sh
# Top 20 slowest classes in the last hour
pt-mongodb-query-digest profile "$URI" --since 1h --limit 20

# Only find/aggregate against the `orders` collection
pt-mongodb-query-digest log mongod.log --ns 'shop.orders' --op find,aggregate

# Only classes that ran COLLSCAN at least 100 times
pt-mongodb-query-digest log mongod.log --min-count 100 --flag collscan

# JSON for a dashboard or pipeline
pt-mongodb-query-digest log mongod.log -o json > digest.json

# Self-contained HTML for a ticket
pt-mongodb-query-digest log mongod.log --html report.html
```

## Filters

| Flag | Example | Meaning |
|---|---|---|
| `--since`, `--until` | `--since 2h`, `--since 2026-04-22T10:00:00Z` | Time window. Suffixes: `s`,`m`,`h`,`d`. |
| `--ns` | `--ns 'shop.orders'`, `--ns 'shop.*'` | Namespace glob. Repeatable. `.` is the db/collection separator. |
| `--op` | `--op find,update` | `find`, `aggregate`, `update`, `delete`, `insert`, `findAndModify`, `count`, `distinct`, `getMore`. |
| `--app` | `--app 'checkout-*'` | `appName` from the driver. |
| `--user` | `--user readonly` | Authenticated user. |
| `--min-ms` | `--min-ms 50` | Only ops at least this many milliseconds. |
| `--min-count` | `--min-count 10` | Only classes with at least N executions. |
| `--flag` | `--flag collscan,regex` | Only classes carrying a given anti-pattern flag. |

Every filter has a `--not-*` counterpart (`--not-ns`, `--not-op`, etc.).

## Anti-pattern flags

Every class is scored against the detector. Flags appear in the static report, JSON output, and web UI, each with a one-line "why this is bad" and a suggested fix.

| Flag | Fires when |
|---|---|
| `collscan` | `planSummary` starts with `COLLSCAN`. |
| `unanchored-regex` | `$regex` that doesn't start with `^`. |
| `large-in` | `$in`/`$nin` array bigger than the large-in threshold (default 200). |
| `unbounded-skip` | `skip()` greater than the unbounded-skip threshold (default 1000). |
| `negation` | Shape contains `$ne` or `$nin`. |
| `plan-flip` | Class has more than one distinct `planCacheKey`. |
| `where` | Uses `$where` (JS eval). |

## Diff mode

Compare two saved runs:

```sh
# Capture the hour before the deploy
pt-mongodb-query-digest log mongod.log \
  --since 2026-04-22T09:00Z --until 2026-04-22T10:00Z \
  --save pre-deploy

# Capture the hour after
pt-mongodb-query-digest log mongod.log \
  --since 2026-04-22T10:00Z --until 2026-04-22T11:00Z \
  --save post-deploy

pt-mongodb-query-digest diff pre-deploy post-deploy
```

The diff report highlights:

- Classes that got slower (by p95, by total time spent).
- New classes that didn't exist in the baseline.
- Classes that stopped firing.
- Plan-cache-key changes on classes present in both.

## History

```sh
pt-mongodb-query-digest log mongod.log --save baseline    # save this run
pt-mongodb-query-digest history list                      # list saved runs
pt-mongodb-query-digest history show baseline             # inspect one
pt-mongodb-query-digest history rm baseline               # delete one
```

History lives in `~/.local/share/pt-mongodb-query-digest/history.db` (SQLite). Override with `--history-db /path/to/file.db`. Nothing is persisted unless you pass `--save` or use the `history` subcommand.

## Web UI

```sh
pt-mongodb-query-digest log mongod.log --web
# opens http://localhost:8080 in your browser
```

Features:
- Sortable table of query classes with inline anti-pattern badges.
- Click a class for detail: histogram of durations, sample query, plan summary, and remediation hints.
- Client-side filter by namespace, op, or flag — no page reload.
- Works offline; no external CDN calls (all assets embedded in the binary).

Flags:
- `--web-addr :8080` — bind address (default `127.0.0.1:8080`).
- `--no-open` — don't auto-launch the browser.

For a shareable artifact:

```sh
pt-mongodb-query-digest log mongod.log --html report.html
```

Produces a single self-contained HTML file (CSS + JS inlined) you can email, attach to a ticket, or open offline.

## Output formats

```sh
pt-mongodb-query-digest log mongod.log -o text       # default, to stdout
pt-mongodb-query-digest log mongod.log -o json       # machine-readable
pt-mongodb-query-digest log mongod.log --html FILE   # self-contained HTML file
pt-mongodb-query-digest log mongod.log --web         # local HTTP server + browser
```

The JSON schema is versioned via a top-level `schema_version` field; breaking changes bump the major.

## Connecting to MongoDB

Standard connection strings:

```sh
pt-mongodb-query-digest profile 'mongodb+srv://user:pass@cluster.example.net/db'
pt-mongodb-query-digest profile 'mongodb://host/db?tls=true&tlsCAFile=/etc/ssl/ca.pem'
pt-mongodb-query-digest profile 'mongodb://host/db?authMechanism=MONGODB-X509&tlsCertificateKeyFile=/etc/ssl/client.pem'
```

To keep credentials out of shell history, export `MONGODB_URI` and invoke with no positional arg:

```sh
export MONGODB_URI='mongodb+srv://user:pass@cluster.example.net/db'
pt-mongodb-query-digest profile
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Ran successfully, report produced. |
| 1 | Any error (bad flag, bad URI, file not found, connection failure, parse error). |

(Finer-grained codes are on the roadmap; for now, the tool writes a one-line error to stderr.)

## FAQ

**Does it modify anything?** No. Read-only on the profiler, read-only on the log. The history DB is local to the machine you run it on.

**Why not just use Atlas / Ops Manager?** Those are great when you have them. This tool is for when you don't — on-prem, dev box, ephemeral container, or a `.log` file someone handed you.

**Text-format (pre-4.4) logs?** Not supported. JSON only.

**mongos logs?** Yes, same parser. Scatter-gather detection requires mongos logs or explicit shard metadata.

**Why SQLite for history?** So the tool stays a single binary with zero runtime dependencies.

## Roadmap

Not yet implemented; called out so you don't hit a surprise:

- **Watch mode** (`--watch --window --refresh`) — live-updating report over a rolling window. Needs a tail path on both inputs and a re-render loop.
- **Markdown output** (`-o markdown`). Trivial from the JSON shape; skipped for MVP.
- **Resume watermark for `profile`** (`--resume FILE`). Currently each run reads the whole filtered range.
- **`unindexed-sort` flag** — requires walking `execStats`, which isn't stored today.
- **`scatter-gather` flag** — requires shard metadata from mongos or config server; mongod-only inputs can't detect it.
- **Pre-4.4 text-format mongod logs** — unsupported by design; open an issue if you need it.
- **Granular exit codes**.

## Status

Pre-1.0. The CLI surface and JSON schema are not yet stable; pin a version if you script against them.
