# Experiment 18 — Maintenance & Observability

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Experiments 01–10 · **Spark profile: not required**

---

## 🎯 The interview question this answers

> **"How do you monitor an Iceberg table in production? What metadata tables does Iceberg expose, and what would you put on a health dashboard? What maintenance jobs should be scheduled and at what cadence?"**

Pause. Open `interview-faq.md`. Write your current answer.

**Why this matters:** Every Iceberg table you'll ever own slowly accumulates debt — small files, snapshot bloat, orphan files, skewed manifests. The teams that operate Iceberg well aren't the ones who avoid the debt; they're the ones who **see** it and have automated jobs that retire it on a schedule. This experiment teaches you what to see and what to schedule.

---

## TL;DR

Iceberg exposes a SQL-queryable **metadata table family** alongside every table: `.snapshots`, `.history`, `.refs`, `.files`, `.entries`, `.manifests`, `.partitions`, `.all_data_files`, `.all_manifests`. Five of them belong on any production dashboard. Three maintenance jobs (`expire_snapshots`, `remove_orphan_files`, `rewrite_data_files` / `rewrite_manifests`) belong on a cron schedule. Get the dashboard + cron right and the table maintains itself.

---

## 🛠️ Hands-on: build a health report

### Step 1 — Reset and create a table with some realistic state

```bash
cd lab && ./reset.sh --confirm
```

In Jupyter:

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType
import pyarrow as pa
from datetime import datetime, timezone, timedelta
import random

catalog = get_catalog()
catalog.create_namespace_if_not_exists("ops")

if "ops.events" in [".".join(t) for t in catalog.list_tables("ops")]:
    catalog.drop_table("ops.events")

schema = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=True),
    NestedField(3, "user_id",  LongType()),
    NestedField(4, "amount",   LongType()),
)
table = catalog.create_table("ops.events", schema=schema)

# Simulate a realistic mess:
#   - one big batch  (healthy file)
#   - 30 tiny commits (small-files debt)
#   - one overwrite  (creates an orphaned data file in history)
#   - one delete     (creates an unreferenced data file)

# Big batch
big = pa.table({
    "event_id": [f"big_{i}" for i in range(5000)],
    "event_ts": [datetime.now(timezone.utc) + timedelta(seconds=i) for i in range(5000)],
    "user_id":  [100 + (i % 50) for i in range(5000)],
    "amount":   [(i % 100) + 1 for i in range(5000)],
})
table.append(big)

# 30 tiny commits
for i in range(30):
    table.append(pa.table({
        "event_id": [f"sm_{i}_0"],
        "event_ts": [datetime.now(timezone.utc)],
        "user_id":  [random.randint(100, 200)],
        "amount":   [random.randint(1, 99)],
    }))

# Overwrite a small slice
table = catalog.load_table("ops.events")
table.overwrite(big)  # replaces everything with the original big batch only

# Reload
table = catalog.load_table("ops.events")
print("Current snapshot:", table.current_snapshot().snapshot_id)
print("Total snapshots :", len(list(table.snapshots())))
```

### Step 2 — Inspect snapshots and history

```python
import pandas as pd
pd.set_option("display.max_columns", None)

snapshots = table.inspect.snapshots().to_pandas()
print(snapshots[[
    "committed_at", "snapshot_id", "parent_id", "operation",
    "summary"
]].head(10))
```

`operation` is the column you'll filter on most: `append`, `overwrite`, `delete`, `replace` (compaction). The `summary` dict carries `added-records`, `deleted-records`, `added-data-files`, `total-records`, etc. — every committed delta in one place.

```python
hist = table.inspect.history().to_pandas()
print(hist)
```

`history` is the linear time-travel view: when each snapshot became `current`. Different from `.snapshots` because rollbacks can mean a snapshot was current more than once.

### Step 3 — Inspect data files and partitions

```python
files = table.inspect.files().to_pandas()
print("Total data files:", len(files))
print("File size stats:")
print(files["file_size_in_bytes"].describe())
print("\nFile content split (0=data, 1=position-delete, 2=equality-delete):")
print(files["content"].value_counts())
```

You should see **1 data file** after the overwrite — but a handful of files in S3 left from earlier snapshots (those are technically unreferenced by the current snapshot but still kept for time travel).

For partitioned tables, `.partitions` gives partition-level rollups:

```python
parts = table.inspect.partitions().to_pandas() if hasattr(table.inspect, "partitions") else None
print(parts)
```

(Our table isn't partitioned, so this is empty or unavailable. Most production tables ARE partitioned; this is where you'll spot skew — one partition with 100x the file count of others.)

### Step 4 — Inspect manifests

```python
manifests = table.inspect.manifests().to_pandas()
print(manifests[[
    "path", "length", "added_data_files_count", "existing_data_files_count",
    "deleted_data_files_count"
]])
```

Manifests are the "intermediate index" layer. A healthy production table has a small number of large manifests; a sick one has many tiny manifests (the result of many small commits — exactly the storm we just created before the overwrite cleaned it up).

### Step 5 — A "table health" report function

This is what you'd run on a schedule:

```python
def table_health(catalog, identifier):
    t = catalog.load_table(identifier)
    snaps = t.inspect.snapshots().to_pandas()
    files = t.inspect.files().to_pandas()
    mans  = t.inspect.manifests().to_pandas()

    current = t.current_snapshot()
    data_files = files[files["content"] == 0]
    delete_files = files[files["content"] > 0]

    report = {
        "identifier": identifier,
        "current_snapshot_id": current.snapshot_id,
        "snapshot_count": len(snaps),
        "snapshots_older_than_7d": int(
            (pd.to_datetime(snaps["committed_at"], utc=True) < pd.Timestamp.utcnow() - pd.Timedelta(days=7)).sum()
        ),
        "data_files": len(data_files),
        "delete_files": len(delete_files),
        "small_files_under_10mb": int((data_files["file_size_in_bytes"] < 10 * 1024 * 1024).sum()),
        "data_file_total_mb": round(data_files["file_size_in_bytes"].sum() / 1024 / 1024, 1),
        "manifest_count": len(mans),
        "manifest_avg_data_files": round(mans["added_data_files_count"].mean() if len(mans) else 0, 1),
    }
    return report

print(table_health(catalog, "ops.events"))
```

Run this against any table and you get a one-row health snapshot. Wire it to a daily job that writes results into a `lakehouse.table_health` Iceberg table, and you have a free history of table health over time.

### Step 6 — Find orphan files

Orphan files are objects in the table's S3 prefix that **no live snapshot references**. They're produced by: failed commits, aborted Spark jobs, deleted-without-purge actions. They cost real money over time.

```python
from src.catalog_helper import list_object_keys

table = catalog.load_table("ops.events")
prefix = table.location().replace("s3://warehouse/", "")
keys = list_object_keys(prefix)
print(f"Total objects in S3 prefix: {len(keys)}")

# Build the set of "live" file paths reachable from the current metadata
def reachable_files(table):
    live = set()
    # current metadata.json
    live.add(table.metadata_location)
    # all snapshots' manifest lists
    for s in table.snapshots():
        if s.manifest_list:
            live.add(s.manifest_list)
        for m in s.manifests(table.io):
            live.add(m.manifest_path)
            for entry in m.fetch_manifest_entry(table.io):
                live.add(entry.data_file.file_path)
    return live

live_paths = reachable_files(table)
live_keys = {p.replace("s3://warehouse/", "") for p in live_paths if p}
all_keys = set(keys)

orphans = sorted(all_keys - live_keys)
print(f"Orphan candidate count: {len(orphans)}")
for k in orphans[:10]:
    print(" ", k)
```

You may see zero orphans on this clean table, or you may see a few from the overwrite. **Production rule:** never delete orphans without a **safety window** — only delete files older than your longest possibly-in-flight commit (recommendation: 7 days). Race conditions between "list objects" and "delete" can otherwise nuke a file an in-flight writer was about to register.

### Step 7 — Schedule maintenance: `expire_snapshots`

PyIceberg API (version-dependent — adjust as needed):

```python
# Expire snapshots older than 7 days, keep last 10 regardless of age
result = table.expire_snapshots(
    retain_last=10,
    older_than=int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000),
).commit()

print("Snapshots expired. Refresh:")
table = catalog.load_table("ops.events")
print("Snapshots now:", len(list(table.snapshots())))
```

(If `expire_snapshots` isn't available on your PyIceberg version, run it via Spark in the spark-profile: `CALL rest_lab.system.expire_snapshots(table => 'ops.events', older_than => …, retain_last => 10)`.)

### Step 8 — Schedule maintenance: rewrite manifests

Many small commits = many small manifests = slow query planning. Rewriting consolidates them.

```python
# PyIceberg: at the time of writing, manifest rewrite is exposed via the maintenance API.
# Spark-CALL equivalent (most portable):
#   CALL rest_lab.system.rewrite_manifests(table => 'ops.events')

# In PyIceberg, the action is typically:
# table.rewrite_manifests()  # if present in your version

manifests_before = table.inspect.manifests().to_pandas()
print("Before:", len(manifests_before), "manifests")

# Try the operation if available
try:
    table.rewrite_manifests()
    table = catalog.load_table("ops.events")
    manifests_after = table.inspect.manifests().to_pandas()
    print("After: ", len(manifests_after), "manifests")
except AttributeError:
    print("rewrite_manifests not in this PyIceberg version — use Spark CALL")
```

### Step 9 — Build a maintenance cron skeleton

```python
SCHEDULE = """
hourly:    expire_snapshots (retain_last=24, older_than=24h)
daily:     rewrite_data_files for partitions with small_files_under_10mb > 100
daily:     rewrite_manifests if manifest_count > 100
weekly:    remove_orphan_files older_than=7d
weekly:    write table_health to ops.table_health
"""
print(SCHEDULE)
```

Real production: this lives in Airflow / Dagster / your favorite orchestrator. The **point** is that there is a schedule. Every table you forget to schedule maintenance for is a table that will silently degrade.

---

## 💥 Break it

### Break 1: query with a stale snapshot pointer

```python
# Take an old snapshot id from before the overwrite
snaps_df = table.inspect.snapshots().to_pandas()
old_snap_id = snaps_df.iloc[0]["snapshot_id"]

# Read at that old snapshot — works (time travel)
df = table.scan(snapshot_id=old_snap_id).to_arrow().to_pandas()
print("Old snapshot rows:", len(df))

# Now expire that snapshot
table.expire_snapshots(retain_last=1, older_than=int(datetime.now(timezone.utc).timestamp() * 1000)).commit()
table = catalog.load_table("ops.events")

# Try to read again
try:
    df = table.scan(snapshot_id=old_snap_id).to_arrow().to_pandas()
except Exception as e:
    print(f"Expected failure: {type(e).__name__}: {e}")
```

**Insight:** `expire_snapshots` is *destructive to time travel*. Any consumer or report pinned to an expired snapshot is now broken. This is exactly why we sized snapshot retention to "max consumer downtime × 3" in experiment 15.

### Break 2: ignore orphan files for a quarter

Don't actually wait three months — just imagine:

```text
Day 0:    1 GB live data, 0 GB orphans
Day 30:   2 GB live data, 0.5 GB orphans
Day 60:   3 GB live data, 1.5 GB orphans
Day 90:   4 GB live data, 4 GB orphans  ← orphans now match live size
```

This is a real curve, not a hypothetical. Streaming sinks, aborted Spark jobs, killed processes — each leaves a few KB-MB of files. At scale these compound. **A quarterly `remove_orphan_files` job is the difference between $200/mo and $2000/mo in S3 storage.**

### Break 3: `remove_orphan_files` with no safety window

```python
# CONCEPTUAL — do NOT run on a table you care about
# Naive orphan cleanup that deletes immediately:
for k in orphans:
    pass  # s3.delete_object(Bucket='warehouse', Key=k)
```

If an in-flight writer was about to register one of those files when you delete it, the writer's commit will succeed (it registers the path in metadata) and then queries will fail because the file doesn't exist. **Always use a safety window** — `older_than` in `remove_orphan_files` should be > your longest possible commit latency. 7 days is industry standard.

### Break 4: monitor manifests, not data files

A table can look healthy on the data-file side (1000 files, all 100MB+ — perfect) and still query slowly because it has 10,000 manifest entries from a year of small commits that were never `rewrite_manifests`'d. Query planning has to read every manifest. **Both layers need monitoring.**

### Break 5: blind compaction

```python
# Compaction is NOT free. A naive nightly "rewrite_data_files on every table"
# in a 100-table lakehouse where 90 tables don't need it = wasted petabytes of IO
# and CPU. Per-partition, threshold-based compaction is the production pattern.
```

The right shape:

```python
def needs_compaction(health_report):
    return (
        health_report["small_files_under_10mb"] > 100
        or health_report["delete_files"] > 50
        or health_report["manifest_count"] > 200
    )
```

Run compaction only on tables that pass this. Cheap to compute, saves orders of magnitude in IO.

---

## 📚 Theory deep-dive

### The metadata table family

Every Iceberg table `T` exposes these as queryable tables `T.<metadata_table>`:

| Metadata table | What's in it | When you query it |
|---|---|---|
| `.snapshots` | one row per snapshot, with `summary` dict | dashboard, debugging "what changed" |
| `.history` | linear "when did each snapshot become current" | time-travel debugging, rollback validation |
| `.refs` | branches and tags, retention metadata | WAP audit trail, expired-ref detection |
| `.files` | one row per *current* data file or delete file | small-files check, file-size distribution |
| `.entries` | one row per manifest entry (live + historical) | deep debugging, "which snapshot added this file" |
| `.manifests` | one row per manifest in the current snapshot | manifest-bloat detection |
| `.partitions` | one row per partition (current snapshot) | partition skew, hot partitions |
| `.all_data_files` | every data file from every snapshot | orphan-file analysis, storage cost forecasting |
| `.all_manifests` | every manifest from every snapshot | same |
| `.metadata_log_entries` | history of metadata.json files written | audit trail of every commit |

PyIceberg exposes most via `table.inspect.*`. Spark/Trino expose them via SQL `SELECT FROM table.<metadata_table>`. **Spend an hour just exploring these against any table you've built** — most engineers learn Iceberg without ever touching this layer, and it's the entire observability surface.

### The five-metric dashboard

If you only get five metrics per table in your dashboard, pick these:

1. **`snapshot_count`** — should plateau under your retention policy. If it grows unbounded, expiration isn't running.
2. **`small_files_pct`** — % of data files under your size target (10 MB threshold is sane). If > 20%, compaction is overdue.
3. **`delete_files`** — non-zero on MoR tables. If > ~5% of data files, you need `rewrite_data_files`.
4. **`orphan_size_bytes`** — output of an orphan scan against the prefix. Watch the *trend*.
5. **`current_snapshot_age_seconds`** — if a table hasn't committed in 24h on what's supposed to be a streaming sink, you have an upstream problem.

Add these as Iceberg columns in a `lakehouse.table_health` table, written hourly by a job that loops over every table you operate. You now have a year's worth of trend data for free.

### The maintenance cron skeleton

| Job | Cadence | What it does | Safe? |
|---|---|---|---|
| `expire_snapshots` | hourly / daily | drops old snapshots from metadata | safe if retention sized correctly |
| `rewrite_data_files` (bin-pack) | daily per partition that needs it | compacts small files | safe under concurrent appends if you use the action API (not `overwrite`) |
| `rewrite_data_files` (sort) | weekly per hot partition | re-sorts for query pruning | same |
| `rewrite_manifests` | weekly when manifest_count > 100 | compacts manifest layer | safe |
| `remove_orphan_files` | weekly with `older_than=7d` | deletes unreferenced S3 objects | safe with safety window; dangerous without |
| `write table_health` | hourly | populates the monitoring table | safe, append-only |

The full list of `CALL system.*` procedures (Spark): `expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`, `rewrite_position_delete_files`, `rewrite_manifests`, `ancestors_of`, `rollback_to_snapshot`, `set_current_snapshot`, `fast_forward`, `cherrypick_snapshot`, `publish_changes`, `register_table`, `migrate`, `snapshot`. PyIceberg implements a subset; for the rest, drop to Spark via the profile.

### What changes once you have these metrics

The thing nobody tells you: **once you can see the metrics, the conversations change**. "Is this table slow?" stops being a guessing game. The 95th-percentile-file-size dropped overnight → small-files storm at midnight → check `snapshots.committed_at` distribution. Done in 3 minutes instead of 3 hours.

A senior data engineer is, more than anything else, **someone who knows what to look at first**. This experiment is about installing those instincts.

### Observability vs alerting

The dashboard is observability. Alerts are a different thing — pick a small number:

- **snapshot_count > 2x expected** — expiration broken
- **delete_files growth > 50%/day** — compaction broken
- **current_snapshot_age > SLA** — writer broken
- **table_health job hasn't run in 2h** — monitoring broken

That's it. Four alerts per table family. Anything more drowns in noise.

---

## ✍️ Re-answer the interview question

> "How do you monitor an Iceberg table in production? What maintenance is scheduled?"

Cover:

1. **Metadata table family** — name 5 (`.snapshots`, `.history`, `.refs`, `.files`, `.manifests`, `.partitions`)
2. **Five-metric dashboard** — snapshot count, small-files %, delete files, orphan bytes, snapshot age
3. **Cron schedule** — expire (hourly), compact (daily, conditional), manifests (weekly), orphans (weekly), health write (hourly)
4. **Safety window** for orphan cleanup
5. **Conditional compaction** — only on tables that fail a health threshold, never blind
6. **Alert sparingly** — four per table family

---

## 🎁 LinkedIn post draft

> **"Most teams discover their Iceberg metadata tables six months too late. Here are five I'd put on every dashboard from day one."**
>
> Walk through the family. End with the conditional-compaction pattern.

---

## Session wrap-up

1. Confirm hands-on: you've run `table_health()` against `ops.events`, you've inspected each metadata table, you have an orphan-detection script that worked.
2. Save your `table_health()` function somewhere reusable — you'll point to it in interviews.
3. Update `interview-faq.md`.
4. Reset / end of day as usual.

---

## Next up

→ [Experiment 19: Performance tuning](19-performance-tuning.md) — once you can *see* the table, you'll learn what to *change* about it.
