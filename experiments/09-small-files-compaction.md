# Experiment 09 — The Small Files Problem & Compaction

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Tiers 1–2 complete

---

## 🎯 The interview question this answers

> **"What's the small file problem? How does Iceberg help, or not? How do you handle compaction in production?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** This is the #1 most common production problem in lakehouses. Every team hits it. If you can articulate the cause, the cost, and the fix — you sound like someone who has run a lakehouse, not just read about one.

---

## TL;DR

Every write creates new files. Streaming ingestion or many small batches → millions of tiny Parquet files. Query planners now have to open millions of files. Performance dies. **Iceberg doesn't prevent this** — it inherits the problem from Parquet. But it gives you the tools to fix it: compaction (rewrite small files into bigger ones) and metadata-aware sorting.

---

## 🛠️ Hands-on: cause the problem, then solve it

### Step 1 — Create the disaster

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestampType
import pyarrow as pa
from datetime import datetime, timezone
import time
import random

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=True),
    NestedField(3, "amount", DoubleType()),
)
table = catalog.create_table("lab.small_files", schema=schema)

# Simulate streaming ingestion: 200 commits, each with 1-5 rows
print("Writing 200 small commits...")
t0 = time.time()
for i in range(200):
    n = random.randint(1, 5)
    data = pa.table({
        "event_id": [f"e_{i}_{j}" for j in range(n)],
        "event_ts": [datetime.now(timezone.utc)] * n,
        "amount":   [random.random() * 100 for _ in range(n)],
    })
    table.append(data)
    if i % 50 == 0:
        print(f"  {i} commits done")

t1 = time.time()
print(f"Done in {t1-t0:.1f}s")
```

### Step 2 — Measure the damage

```python
table = catalog.load_table("lab.small_files")

# How many data files?
files = list(table.scan().plan_files())
print(f"Data files: {len(files)}")

# Total rows?
total_rows = sum(f.file.record_count for f in files)
print(f"Total rows: {total_rows}")

# Average file size?
total_bytes = sum(f.file.file_size_in_bytes for f in files)
avg_bytes = total_bytes / len(files)
print(f"Average file size: {avg_bytes:.0f} bytes ({avg_bytes/1024:.1f} KB)")
print(f"Average rows per file: {total_rows / len(files):.1f}")
```

You should see **200 files, ~600 total rows, ~1KB each**. This is catastrophic for production. **A healthy file size is 100MB–1GB.**

### Step 3 — Measure the query cost

```python
# Time a full scan
import time
t0 = time.time()
result = table.scan().to_arrow()
t1 = time.time()
print(f"Full scan: {len(result):,} rows in {(t1-t0)*1000:.0f} ms")

# How many manifests does the snapshot have?
snapshot = table.current_snapshot()
print(f"Snapshot ID: {snapshot.snapshot_id}")
# Try inspecting manifests
manifests = table.inspect.manifests().to_pandas()
print(f"Manifests: {len(manifests)}")
print(manifests[["path", "length", "added_data_files_count"]].head())
```

You'll likely see ~200 manifests as well — one per commit. Both data file count and manifest count are blown out.

### Step 4 — Compact data files

In PyIceberg's current API, file compaction is exposed via `rewrite_data_files`. The exact API varies by version — check `pyiceberg` docs. Here's the conceptual flow:

```python
# Approach 1: PyIceberg's rewrite action (when available)
# table.rewrite_data_files(target_file_size_in_bytes=128_000_000)

# Approach 2: read everything, overwrite with a single batch
# (this is the manual equivalent; in production you'd use Spark or PyIceberg's action)

table = catalog.load_table("lab.small_files")
all_data = table.scan().to_arrow()
print(f"Read {len(all_data):,} rows into memory")

# Overwrite the table with the consolidated data
table.overwrite(all_data)
table = catalog.load_table("lab.small_files")

# Verify
files_after = list(table.scan().plan_files())
print(f"Data files after compaction: {len(files_after)}")
total_bytes_after = sum(f.file.file_size_in_bytes for f in files_after)
print(f"Total size: {total_bytes_after / 1024:.1f} KB across {len(files_after)} files")
```

You should see 1 file (or a handful), consolidated. Re-run the timed scan:

```python
t0 = time.time()
result = table.scan().to_arrow()
t1 = time.time()
print(f"Full scan AFTER compaction: {len(result):,} rows in {(t1-t0)*1000:.0f} ms")
```

You should see a meaningful speedup, especially noticeable as table size grows.

### Step 5 — Compact manifests separately

Even after data file compaction, you may have many old manifests in the snapshot history. Manifests can also be compacted:

```python
# Conceptual — exact API depends on pyiceberg version
# table.rewrite_manifests()

# Inspect manifest count
print("Manifests:", len(table.inspect.manifests().to_pandas()))
```

In production, both rewrites are typically scheduled separately. **Data file compaction is per-partition; manifest compaction is per-table.**

### Step 6 — Sort within files for better pruning

Compaction is more valuable when you sort data within files. A file with values 1-1000 has tight min/max bounds; a file with random values has wide bounds (and won't be pruned).

Set a sort order on the table:

```python
# Pseudo-code — exact API varies
# from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
#
# with table.update_sort_order() as us:
#     us.asc("event_ts")

# When compaction runs next, new files will be sorted on event_ts.
# Files sorted on event_ts can be pruned by event_ts filters with tight bounds.
```

**Insight:** sort order is a powerful tool for query optimization that most teams underuse. Combined with compaction, it can speed up filtered queries by 10x+.

---

## 💥 Break it

### Break 1: write 10,000 single-row files and watch query planning explode

```python
# DON'T actually run this on a slow lab — but conceptually:
# for i in range(10000):
#     table.append(pa.table({"event_id": [f"e{i}"], "event_ts": [now()], "amount": [1.0]}))
#
# Then time a scan. You'll see seconds spent in PLANNING before any data is read.
```

**Insight:** small files hurt **planning time** as much as **read time**, often more. Iceberg's manifest layer mitigates this but doesn't eliminate it.

### Break 2: compact while writes are happening

```python
# Open a second terminal that keeps writing
# In your main session, run compaction (overwrite + sort)
# Does the concurrent writer's data survive?
```

If you use `overwrite` carelessly, **you can lose concurrent writes**. This is why production systems use Iceberg's `rewrite_data_files` action (which is designed to be safe under concurrent appends) rather than naive overwrite-everything.

**Lesson:** in production, never use `overwrite` for compaction unless you can guarantee no concurrent writes. Use the rewrite actions.

### Break 3: compact too aggressively

If you compact every hour and you have 1TB tables, you're doing 1TB of write IO every hour to consolidate maybe 10MB of new data. **The cure becomes worse than the disease.**

Healthy compaction policy: compact partitions that have many small files AND haven't been compacted recently. Don't compact partitions that are already healthy.

---

## 📚 Theory deep-dive

### Why small files hurt

Two costs:

**1. Per-file overhead.** Each file open has:
- S3 GET request (~10-50ms each)
- Parquet footer parsing
- Row group metadata reading

If you have 10,000 files of 1KB each vs. 10 files of 1MB each, the per-file overhead dominates by 1000x.

**2. Metadata bloat.** Each data file = an entry in a manifest. Each manifest = an entry in a manifest list. More files = more metadata to read at plan time. Even with Iceberg's pruning, planning slows down.

**Target:** 100MB–1GB per file. Below 50MB = too small. Above 2GB = too large (slows down parallelism).

### Compaction strategies

**Bin packing** (most common):
- Group small files into bins of ~target_size
- Rewrite each bin as one file
- Preserves data ordering within bins
- Cheap, safe, broadly applicable

**Sort-based** (better for query performance):
- Read all files in a partition
- Sort rows by sort order columns
- Write back as size-targeted files
- More expensive, but gives much better pruning later

**Z-order / Hilbert curves** (advanced):
- Multi-dimensional sort that preserves locality across multiple columns
- Used by Delta Lake; emerging in Iceberg ecosystem
- Best for queries that filter on multiple dimensions

**Decision:** start with bin packing. Add sort-based for hot partitions. Z-order only if you have very specific multi-column filter patterns.

### Compaction scheduling in production

Common patterns:

1. **Periodic table-wide compaction**: simple, but wastes work on already-compact partitions
2. **Per-partition trigger based on file count**: "if partition has > 100 small files, compact it"
3. **Watermark-based**: "compact partitions older than X hours, freeze them"

The third pattern is especially good for time-series tables: hot partitions (today) keep accepting fast writes, cold partitions (yesterday and older) get compacted to large sorted files.

### The streaming ingestion pattern

Streaming ingestion is the small-files factory. The fix is a two-stage architecture:

```
Streaming source (Kafka)
    ↓
Bronze table (Iceberg) — many small files, accept fast writes
    ↓
Compaction job (every N minutes)
    ↓
Silver table (or same table after compaction) — large optimized files
    ↓
Analytics queries
```

**Or, with Iceberg specifically:** use the streaming sink's "commit batching" feature (e.g., Flink-Iceberg sink batches writes into larger commits) to avoid the problem upstream.

### Cost economics

Compaction is **not free**:
- Compute cost to read and rewrite
- S3 PUT requests (charged per request and per byte)
- Storage cost for the new files (until old ones are expired)

A rough model: compaction every hour on a 1TB table that grows 10GB/hour = ~$50/day in AWS. Worth it only if query performance benefits outweigh it.

**Decision framework:**
- High-read, low-write tables → compact aggressively
- Low-read, high-write tables → compact less frequently
- Streaming + analytics on same table → use bronze/silver split

---

## 🇪🇺 Regulatory angle

Compaction interacts with **DORA Article 28** (ICT third-party risk) and **MiFID II RTS 25** (audit trail) in interesting ways:

**Audit trail preservation.** When you compact files, the original files are no longer referenced by current snapshots but **they still exist** until `expire_snapshots` removes them. This means: compaction does not break time travel. A regulator asking "show me the data as of last Tuesday" still works after a Wednesday compaction.

**Data lineage.** Each compaction creates a new snapshot with a `summary.operation = "replace"`. The summary records that this was a compaction, not a data change. Good for auditability — a regulator can distinguish "data changed" from "data reorganized."

**Cost discipline as a regulatory concern.** DORA requires operational resilience, including cost management. Over-aggressive compaction = high cloud cost = vulnerability if cloud spend isn't controlled. Designing a sensible compaction policy is itself an operational risk control.

In interviews: "Our compaction policy preserves snapshot history for the regulatory retention period and uses operation summaries to maintain a clean audit trail distinguishing data changes from reorganizations. The retention window is set to meet MiFID II RTS 25 requirements; compaction frequency is bounded by our DORA-aligned cost ceiling."

---

## ✍️ Re-answer the interview question

> "What's the small file problem? How do you handle compaction in production?"

Cover:

1. Cause: every write = new files; streaming or many small batches = file count explosion
2. Cost: per-file open overhead + metadata bloat in manifests
3. Iceberg doesn't prevent it (inherited from Parquet) but provides tools to fix it
4. Tools: `rewrite_data_files`, `rewrite_manifests`, sort orders
5. Strategy: bin packing as default, sort-based for query-hot partitions, watermark scheduling
6. Cost tradeoff: don't over-compact; bronze/silver split for streaming

---

## 🎁 LinkedIn post draft

> **"The #1 production issue I've seen with lakehouse adoption isn't query performance — it's the small files problem. Here's the playbook I use to manage it."**
>
> Walk through measurement → compaction → scheduling. End with cost-aware decision framework.

This kind of "operational war stories" post performs very well with senior data engineers and engineering managers — exactly your hiring audience.

---

## Session wrap-up (close the loop)

1. Confirm hands-on is done: small-file storm measured, compaction (or rewrite strategy) applied, file count before/after recorded. Optional notebook: `lab/notebooks/06_compaction.ipynb`.
2. Update `interview-faq.md` with your **after** answer to this experiment's interview question.
3. If Break it left the lab in a broken state: `cd lab && ./reset.sh --confirm` — see [operation guide §7](../docs/operation-guide.md).
4. **End of day** (pause until tomorrow, keep data): `cd lab && docker compose stop` — [operation guide §8](../docs/operation-guide.md).
5. **Done with the lab on this machine** (remove all local tables and catalogs): [operation guide §10](../docs/operation-guide.md).

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md).

---

## Next up

→ [Experiment 10: Snapshot expiration & GC](10-expiration-gc.md) — the other side of file management. When and how to actually delete old data.
