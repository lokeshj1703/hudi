---
name: timeline-inspector
description: Run the Hudi TimelineInspector standalone Java tool against a Hudi table to inspect commits, find file-id lifecycle events, dump raw archive entries, or get per-commit stats. Use when the user wants timeline forensics on a Hudi table — phrases like "use TimelineInspector", "inspect the timeline", "show commit stats for table …", "find file id …", "dump archive entry for instant …", "parse this Hudi filename". Assumes Claude is running locally (laptop or bastion host) with `spark-submit` typically available via $SPARK_HOME.
---

# TimelineInspector — Running the Tool

This skill helps run the `org.apache.hudi.tools.TimelineInspector` CLI against a Hudi table and present the results. It only covers **running** the tool — pick the mode, assemble the classpath, invoke, and format output. It does not do automated forensic analysis (e.g. correlating rollbacks with validator failures); that's a separate playbook.

The tool itself lives in `hudi-internal` at `hudi-common/src/main/java/org/apache/hudi/tools/TimelineInspector.java`, with a runbook at `hudi-common/src/main/java/org/apache/hudi/tools/TimelineInspector.md`. Always read the runbook in the user's checkout if you're unsure about a flag — it's authoritative.

## Step 1 — Decide which mode the user needs

Map the user's request to one of these six modes. If ambiguous, ask.

| User intent | Mode |
|---|---|
| "Show commit stats", "what was written between X and Y", "how many inserts/updates", "latest N commits" | `--commit-stats` |
| "Summarize ingestion duration split", "where is the time going on ingest", "checkpoint SLA breakdown", "per-phase wall-clock for last N commits", "how long does the MDT write take" | `--phase-timings` |
| "Inspect instant 20260530…", "show all states of …", "dump metadata for instant" | `--show-instant` |
| "Where did file id X come from", "what happened to file Y", "find rollback that touched commit Z", "file group lifecycle" | `--find-file-id` (add `--lifecycle` for the collapsed view) |
| "Decode this filename", "what's the commit time / fileId in `xxx.parquet`" | `--parse-filename` (no base path needed) |
| "Show me the full archive entry for instant …", "`--show-instant` returned (no body)" | `--raw-archive` |

## Step 2 — Gather required inputs

Always required:
- `--base-path` — Hudi table base path (local fs, `s3://…`, `gs://…`, or `abfs://…`). Skip for `--parse-filename`.

Mode-specific arguments:
- `--show-instant <ts>`, `--find-file-id <id-or-filename-or-instant>`, `--raw-archive <ts>`, `--parse-filename <name>`.

Common modifiers worth offering when relevant:
- `--start-instant` / `--end-instant` — narrow the timeline window (always recommended for `--commit-stats` on long-lived tables; the tool warns about `loadCompletedInstantDetailsInMemory()` otherwise).
- `--no-archived` — skip archived timeline (faster, sometimes sufficient).
- `--limit N` — defaults to 100; bump up for `--find-file-id` deep dives.
- `--sort asc|desc` — for `--commit-stats` / `--phase-timings`; default `desc` returns latest N within range.
- `--actions <csv>` — restrict to specific actions (`commit,deltacommit,replacecommit,clean,rollback,restore,compaction,logcompaction,savepoint`). For `--commit-stats` and `--phase-timings` only the three ingest actions are allowed.
- `--state REQUESTED|INFLIGHT|COMPLETED` — for `--show-instant` only.
- `--lifecycle` — for `--find-file-id` only; collapses to CREATED / REPLACED / CLEANED / ROLLED_BACK rows.
- `--include-replacecommit` — for `--phase-timings` only; opts in to reporting clustering / insert-overwrite alongside ingest.
- `--output table|json` — JSON is easier to grep/jq; default `table`.
- `--quiet` / `-q` — suppress per-instant deserialization warnings.

If the user didn't specify a window for `--commit-stats` on a "production" table, **proactively suggest** narrowing with `--start-instant` / `--end-instant` or `--no-archived` before running — archived-timeline scans without a window load everything into memory.

## Step 3 — Pick the invocation

The tool needs `hudi-common` + Hadoop + Jackson on the classpath. The Hudi bundles mark Hadoop/Jackson as `provided`, so a bare `java -cp <bundle>` will fail with `NoClassDefFoundError: org/apache/hadoop/fs/FileSystem`.

### Preferred — `spark-submit` (when `$SPARK_HOME` is available)

Check `$SPARK_HOME`:
```bash
echo "$SPARK_HOME"
```

If set and the directory exists, use it. Locate a Hudi bundle (any of `hudi-utilities-bundle_2.12-0.14.1-rc2.jar`, `hudi-spark3.5-bundle_2.12-0.14.1-rc2.jar`, `hudi-cli-bundle_2.12-0.14.1-rc2.jar` works). If the user didn't specify a path, check the usual locations in the hudi-internal checkout:
```bash
ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_*.jar 2>/dev/null
ls packaging/hudi-spark-bundle/target/hudi-spark*-bundle_*.jar 2>/dev/null
ls packaging/hudi-cli-bundle/target/hudi-cli-bundle_*.jar 2>/dev/null
```

Then invoke:
```bash
spark-submit \
  --class org.apache.hudi.tools.TimelineInspector \
  --master "local[1]" \
  --conf spark.log.level=WARN \
  --driver-memory 4g \
  <BUNDLE_JAR> \
  --base-path <BASE_PATH> \
  <MODE_FLAGS>
```

- `--master "local[1]"` — never `yarn` / `k8s://…`; we're using spark-submit as a classpath resolver, not running a real Spark app.
- `--conf spark.log.level=WARN` — silences Spark's INFO banner so the tool's table output is readable.
- `--driver-memory 4g` (bump to `8g`) — only needed if archived timeline is included with no time window.
- For S3/GCS base paths: add `--packages org.apache.hadoop:hadoop-aws:3.3.4` (or the matching `gcs-connector` / `hadoop-azure` artifact).

### Fallback 1 — `$SPARK_HOME` not set, but Hadoop is installed

```bash
hadoop jar <BUNDLE_JAR> org.apache.hudi.tools.TimelineInspector --base-path <BASE_PATH> <MODE_FLAGS>
```

`hadoop jar` prepends `$(hadoop classpath)` automatically.

### Fallback 2 — `$SPARK_HOME` not set, Hadoop not on PATH, but a Spark tarball is on disk

```bash
java -cp "<BUNDLE_JAR>:<SPARK_DIR>/jars/*" \
  org.apache.hudi.tools.TimelineInspector \
  --base-path <BASE_PATH> <MODE_FLAGS>
```

`<SPARK_DIR>` must be a `spark-X.Y.Z-bin-hadoopN` build (not the "without-hadoop" flavor). Quote the entire `-cp` value so the shell doesn't expand `dir/*` — that's a JVM glob.

### Fallback 3 — Nothing installed, but `hudi-internal` is checked out and built

```bash
mvn -pl hudi-common dependency:build-classpath -DincludeScope=test \
  -Dmdep.outputFile=/tmp/ti_cp.txt

java -cp "hudi-common/target/hudi-common-0.14.1-rc2.jar:$(cat /tmp/ti_cp.txt)" \
  org.apache.hudi.tools.TimelineInspector \
  --base-path <BASE_PATH> <MODE_FLAGS>
```

The Maven step is one-shot; the `$CP` string is reusable until deps change.

### If none of the above is available

Tell the user what's missing and suggest the cheapest path forward:
1. Easiest: install a Spark tarball (`spark-3.5.0-bin-hadoop3`), set `$SPARK_HOME`, retry with the preferred recipe.
2. If they have a `hudi-internal` checkout, run `mvn -pl hudi-common -am package -DskipTests` to populate `~/.m2`, then use Fallback 3.

## Step 4 — Run and present output

When running:
- Show the user the full command you're about to execute before running it.
- Use `--quiet` by default to suppress per-instant deserialization warnings — they're noise unless the user is actively debugging archive corruption.
- For `--commit-stats` and `--show-instant`, prefer `--output table` for human reading; switch to `--output json` if the user wants to pipe / grep.

When presenting output:
- For `--commit-stats`: keep the tool's table as-is, but call out the footer (`totals` + `avg per commit`) explicitly. If the user asked a specific question ("how many records did we ingest yesterday?") answer it from the totals row rather than making them read the table.
- For `--phase-timings`: lead with the per-action footer aggregates (mean / p50 / p95 / max per phase + `share of total`) — that's the actionable summary. The per-instant table is useful for spotting outliers but the aggregates answer "where is the time going?" directly. Always mention the `skipped` count if non-zero; it indicates rolled-back or partial writes you might want to investigate separately. If the user asked about a specific SLA ("can we hit 5 min checkpoints?"), use the p95 of `total_ms`, not mean.
- For `--find-file-id`: if the user wanted a lifecycle view, default to `--lifecycle`. Otherwise the raw events table is dense — call out the `matchType` column (e.g. `writeStat`, `replaceFileId`, `cleanSuccessDelete`, `rollbackOfCommit`) and explain what each one means if the user is unfamiliar.
- For `--show-instant` / `--raw-archive`: if output is large, summarize the structure ("3 states found: REQUESTED, INFLIGHT, COMPLETED — COMPLETED metadata has N write stats across M partitions") before pasting the full body.
- For `--parse-filename`: just show the parsed key/value output — there's nothing to interpret.

## Common failure modes

| Symptom | Fix |
|---|---|
| `NoClassDefFoundError: org/apache/hadoop/fs/FileSystem` | Bare `java -cp <bundle>` — switch to `spark-submit` or Fallback 1/2. |
| `NoClassDefFoundError: com/fasterxml/jackson/datatype/jsr310/JavaTimeModule` | Bundle missing Jackson `jsr310`. Same fix — use one of the recipes above. |
| `(no archive entries with commitTime=…)` | Instant isn't in any archive log under `.hoodie/archived/`. Try `--show-instant` without `--no-archived` — it might still be on the active timeline. |
| `IllegalAccessError` / `module … does not "opens" …` on Java 11+ | Add Spark's standard opens: `--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED`. |
| OOM with archived timeline | Pass `--start-instant` + `--end-instant`, or `--no-archived`. Both `--commit-stats` and `--find-file-id` honor those filters. |
| Per-instant `WARN: failed to process …` lines | Add `-q` / `--quiet` if not actively debugging archive corruption. |

## Quick reference — invocations by mode

Replace `$BUNDLE` with a Hudi bundle jar path and `$SS` with `spark-submit --class org.apache.hudi.tools.TimelineInspector --master "local[1]" --conf spark.log.level=WARN`.

```bash
# Per-commit stats, latest 50, active timeline only
$SS $BUNDLE --base-path /tmp/tbl --commit-stats --no-archived --limit 50

# Per-commit stats for a calendar week, JSON output
$SS $BUNDLE --base-path s3://bucket/tbl --commit-stats \
  --start-instant 20260601000000000 --end-instant 20260608000000000 \
  --output json --quiet

# Per-phase wall-clock split for the last 10 ingest commits (always active timeline)
$SS $BUNDLE --base-path /tmp/tbl --phase-timings --limit 10 --quiet

# Phase timings for a calendar hour including clustering, JSON output
$SS $BUNDLE --base-path s3://bucket/tbl --phase-timings --include-replacecommit \
  --start-instant 20260601150000000 --end-instant 20260601160000000 \
  --output json --quiet

# All states of one instant
$SS $BUNDLE --base-path /tmp/tbl --show-instant 20260530092852891

# File id lifecycle (collapsed)
$SS $BUNDLE --base-path /tmp/tbl --find-file-id <fileId> --lifecycle --quiet

# Find which rollback reverted an ingest commit
$SS $BUNDLE --base-path /tmp/tbl --find-file-id <ingestCommitTs> \
  --actions rollback,restore --output json --quiet

# Decode a filename (no base path)
$SS $BUNDLE --parse-filename '<fileId>_<token>_<commit>.parquet'

# Dump every sibling field of an archived instant
$SS $BUNDLE --base-path /tmp/tbl --raw-archive 20260504091819937 --quiet
```
