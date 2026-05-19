# Experiment 19 — Performance Tuning

> **Time:** 120 min · **Tier:** Production · **Prerequisites:** Experiments 01–10, 18 strongly recommended · **Spark profile: optional** (PyIceberg + DuckDB cover the read side; Spark cleans up the `CALL` syntax for some operations)

---

## 🎯 The interview question this answers

> **"A user says an Iceberg table is slow. What's your diagnosis order? What knobs do you turn — partition spec, sort order, file size, write distribution, bloom filters? When does each one matter?"**

Pause. Open `interview-faq.md`. Write your current answer.

**Why this matters:** "It's slow" is the most common ticket against a lakehouse. Most candidates jump to "add more partitions" — which is the wrong answer 70% of the time. The right answer is a *diagnosis order*: first check planning (file count, manifest count), then file selection (sort order, bloom filters), then read cost (file size, row group size). Knowing this order signals senior.

---

## TL;DR

Three layers of cost in an Iceberg query:

1. **Plan cost** — opening the manifests to figure out which files to read. Hurts with too many small files, too many manifests, or no manifest-level pruning.
2. **File-selection cost** — once you know your candidate files, pruning them via min/max stats. Hurts when sort order is wrong, when data is random, or when bloom filters aren't set on filtered columns.
3. **Scan cost** — actually reading the chosen Parquet row groups. Hurts when files are too big (low parallelism) or too small (per-file overhead).

The four knobs that move the needle: **partition spec**, **sort order**, **target file size**, **write distribution mode**. Bloom filters are a tactical fifth knob for high-cardinality filters.

---

## 🛠️ Hands-on: measure first, tune second

### Step 1 — Reset and create a 100k-row table with realistic shape

```bash
cd lab && ./reset.sh --confirm
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType, DoubleType
import pyarrow as pa
from datetime import datetime, timezone, timedelta
import random, time

catalog = get_catalog()
catalog.create_namespace_if_not_exists("perf")

schema = Schema(
    NestedField(1, "txn_id",   StringType(), required=True),
    NestedField(2, "user_id",  LongType(),   required=True),
    NestedField(3, "merchant", StringType()),
    NestedField(4, "country",  StringType()),
    NestedField(5, "amount",   DoubleType()),
    NestedField(6, "txn_ts",   TimestampType(), required=True),
)

if "perf.unsorted" in [".".join(t) for t in catalog.list_tables("perf")]:
    catalog.drop_table("perf.unsorted")
unsorted = catalog.create_table("perf.unsorted", schema=schema)

random.seed(42)
N = 100_000
base = datetime(2026, 5, 1, tzinfo=timezone.utc)

# 100 commits of 1000 rows each (a streaming-ish shape) — random user_id, random country
for i in range(100):
    rows = pa.table({
        "txn_id":   [f"t_{i}_{j}" for j in range(1000)],
        "user_id":  [random.randint(1, 50_000) for _ in range(1000)],
        "merchant": [random.choice(["Amazon","DBahn","Lidl","REWE","Edeka","Aldi","Apple","SBahn"]) for _ in range(1000)],
        "country":  [random.choice(["DE","FR","NL","ES","IT","PL","AT","BE"]) for _ in range(1000)],
        "amount":   [round(random.random()*200, 2) for _ in range(1000)],
        "txn_ts":   [base + timedelta(seconds=random.randint(0, 30*24*3600)) for _ in range(1000)],
    })
    unsorted.append(rows)

unsorted = catalog.load_table("perf.unsorted")
print("Snapshots:", len(list(unsorted.snapshots())))
print("Data files:", len(list(unsorted.scan().plan_files())))
```

### Step 2 — Measure: how bad is "unsorted, small-files-y"?

```python
def time_query(table, predicate_str, runs=3):
    """Return median ms across runs."""
    from pyiceberg.expressions import EqualTo, GreaterThan, And
    # Build a simple equality scan
    import time
    durations = []
    for _ in range(runs):
        t0 = time.time()
        df = table.scan(row_filter=predicate_str).to_arrow()
        t1 = time.time()
        durations.append((t1 - t0) * 1000)
    durations.sort()
    return durations[len(durations)//2], len(df)

ms, rows = time_query(unsorted, "country = 'DE'")
print(f"country=DE: {ms:.0f} ms, {rows} rows")

ms, rows = time_query(unsorted, "user_id = 12345")
print(f"user_id=12345: {ms:.0f} ms, {rows} rows")

ms, rows = time_query(unsorted, "txn_ts >= '2026-05-15T00:00:00'")
print(f"txn_ts >= 2026-05-15: {ms:.0f} ms, {rows} rows")
```

Record the numbers — these are your **before** baseline.

Also note from experiment 18's playbook:

```python
files = unsorted.inspect.files().to_pandas()
print("data file count:", (files['content']==0).sum())
print("avg size MB    :", files[files['content']==0]['file_size_in_bytes'].mean()/1024/1024)
print("median size KB :", files[files['content']==0]['file_size_in_bytes'].median()/1024)
```

You'll likely see ~100 files of ~20-40 KB each. Catastrophically small.

### Step 3 — Tune knob 1: file size (via compaction)

```python
# Compact via overwrite. In production, use rewrite_data_files (action API).
all_data = unsorted.scan().to_arrow()
unsorted.overwrite(all_data)

unsorted = catalog.load_table("perf.unsorted")
files = unsorted.inspect.files().to_pandas()
print("After compaction:")
print(" files:", (files['content']==0).sum())
print(" avg MB:", round(files[files['content']==0]['file_size_in_bytes'].mean()/1024/1024, 2))
```

Re-run the timings:

```python
print("Compacted, unsorted:")
for pred in ["country = 'DE'", "user_id = 12345", "txn_ts >= '2026-05-15T00:00:00'"]:
    ms, n = time_query(unsorted, pred)
    print(f"  {pred}: {ms:.0f} ms, {n} rows")
```

**Observation:** the country and user_id queries should be slightly faster (less plan time), but not dramatically — because there's still no sort order helping you prune files.

### Step 4 — Tune knob 2: sort order

Create a sorted version of the same table:

```python
if "perf.sorted_by_ts" in [".".join(t) for t in catalog.list_tables("perf")]:
    catalog.drop_table("perf.sorted_by_ts")

# Build the sort order using PyIceberg
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import IdentityTransform

sort_order = SortOrder(
    SortField(source_id=6, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST),
)

sorted_table = catalog.create_table(
    "perf.sorted_by_ts",
    schema=schema,
    sort_order=sort_order,
)

# Write the data sorted by txn_ts
sorted_data = all_data.sort_by("txn_ts")
sorted_table.append(sorted_data)
sorted_table = catalog.load_table("perf.sorted_by_ts")

print("Sorted by txn_ts. Files:", len(list(sorted_table.scan().plan_files())))
```

Now compare the `txn_ts >= '2026-05-15'` query:

```python
print("Sorted by txn_ts:")
for pred in ["country = 'DE'", "user_id = 12345", "txn_ts >= '2026-05-15T00:00:00'"]:
    ms, n = time_query(sorted_table, pred)
    print(f"  {pred}: {ms:.0f} ms, {n} rows")
```

**The txn_ts query should be much faster on the sorted table** — Iceberg can prune entire row groups (and possibly entire files, if you compact into multiple files later) using the file-level min/max stats. The country and user_id queries are *not faster* because the sort key didn't help them — sort order is a one-dimensional optimization.

### Step 5 — Tune knob 3: partition spec

A sort order helps within a file. A partition spec helps across files (and across the file-listing step). Add a partition by `country`:

```python
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

if "perf.by_country" in [".".join(t) for t in catalog.list_tables("perf")]:
    catalog.drop_table("perf.by_country")

partition_spec = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="country"),
)

partitioned = catalog.create_table(
    "perf.by_country",
    schema=schema,
    partition_spec=partition_spec,
    sort_order=sort_order,
)

# PyIceberg should fan-out writes per partition
partitioned.append(sorted_data)
partitioned = catalog.load_table("perf.by_country")

print("Partitioned files:", len(list(partitioned.scan().plan_files())))
```

You should see ~8 files now (one per country). Query timings:

```python
print("Partitioned by country, sorted by txn_ts:")
for pred in ["country = 'DE'", "user_id = 12345", "txn_ts >= '2026-05-15T00:00:00'"]:
    ms, n = time_query(partitioned, pred)
    print(f"  {pred}: {ms:.0f} ms, {n} rows")
```

`country = 'DE'` should now be much faster — Iceberg can prune 7 of 8 files using the partition summary in the manifest list, *without opening them*.

### Step 6 — Tune knob 4: write distribution mode

(This knob is mostly Spark-side and matters during *writes*, not queries. Worth understanding.)

Three options:

- **`none`** — each task writes its own file. Maximum parallelism, may produce many small files per partition.
- **`hash`** — repartition by partition key before write. One file per (partition, task). Common default.
- **`range`** — order data globally so files are non-overlapping ranges. Best for query pruning, costs an extra shuffle.

```python
# In PyIceberg today, write distribution is implicit. In Spark (in spark-profile):
# ALTER TABLE perf.by_country SET TBLPROPERTIES ('write.distribution-mode' = 'hash');
# INSERT INTO perf.by_country SELECT * FROM source;
```

Rule of thumb:
- **none** for streaming bronze tables — files will be small but compaction will catch up
- **hash** as the default for batch writes — one file per partition per task
- **range** for tables that get heavy filtered reads — pays back in query time

### Step 7 — Tune knob 5: bloom filters for high-cardinality lookups

`user_id` is high-cardinality (50k unique values). Sort order on `txn_ts` doesn't help filter on `user_id`. Bloom filters can.

```python
# Set table properties to enable bloom filters on user_id
# (PyIceberg supports this via table properties; Spark exposes ALTER TABLE)

# Conceptually:
# ALTER TABLE perf.sorted_by_ts SET TBLPROPERTIES (
#     'write.parquet.bloom-filter-enabled.column.user_id' = 'true',
#     'write.parquet.bloom-filter-max-bytes' = '1048576'
# );

# Then rewrite the data so new files include bloom filters.
```

In PyIceberg today (0.11), bloom filter writing depends on PyArrow's Parquet writer settings — you set them via the `parquet` write properties. Setting via Spark in the spark-profile is the most reliable path.

Once bloom filters are present, `user_id = 12345` queries skip many row groups instead of scanning them. Expect 5-10x improvement on point lookups.

### Step 8 — Compare all four versions side by side

```python
import pandas as pd

results = []
for name, t in [
    ("unsorted_compacted", unsorted),
    ("sorted_by_ts", sorted_table),
    ("partitioned_country", partitioned),
]:
    for pred in ["country = 'DE'", "user_id = 12345", "txn_ts >= '2026-05-15T00:00:00'"]:
        ms, n = time_query(t, pred)
        results.append({"table": name, "predicate": pred, "ms": ms, "rows": n})

print(pd.DataFrame(results).pivot(index="predicate", columns="table", values="ms"))
```

You should see something like:

```
                                       unsorted_compacted   sorted_by_ts   partitioned_country
country = 'DE'                                  120              115                  15
user_id = 12345                                 110              108                  90
txn_ts >= '2026-05-15T00:00:00'                 130               40                  40
```

(Exact numbers vary — the *pattern* is what matters.)

### Step 9 — The diagnosis order in action

A user reports `user_id = 12345` is slow. Walk the diagnosis:

```python
# 1. Plan cost? Check file & manifest count.
files = sorted_table.inspect.files().to_pandas()
manifests = sorted_table.inspect.manifests().to_pandas()
print("files:", len(files), "manifests:", len(manifests))

# 2. File selection? Look at what plan_files actually returns for the predicate.
plan = list(sorted_table.scan(row_filter="user_id = 12345").plan_files())
print(f"plan returned {len(plan)} files for user_id = 12345")
print(f"= scanned {sum(f.file.file_size_in_bytes for f in plan)/1024/1024:.1f} MB to find a probable few rows")

# 3. Scan cost? File sizes.
print("file sizes:", sorted([f.file.file_size_in_bytes for f in plan]))
```

In this case the plan returns ALL files for `user_id = 12345` because the sort is on `txn_ts`, not `user_id`. The diagnosis points at: **bloom filter on user_id** (cheap to enable, big win for point lookups) or **co-sort by user_id, txn_ts** if user_id is the dominant filter (more invasive, only worth it if the workload supports it).

---

## 💥 Break it

### Break 1: over-partition

```python
# DON'T actually create this — but imagine PARTITIONED BY (txn_ts) without a transform
# That's one partition per microsecond. Tens of millions of partitions. Manifest list
# becomes unusable; planning explodes.
```

This is the most common Iceberg disaster. Partition spec should use **transforms** (`days(txn_ts)`, `bucket(16, user_id)`, `truncate(10, country)`) to keep partition cardinality in the 100–10,000 range. **Production rule:** target 100MB–10GB *per partition*, not per file.

### Break 2: sort key that nobody filters on

A team adds `sort by amount` because "compaction takes ages anyway, might as well sort." Nobody filters by amount in production. The sort cost is paid every compaction; the benefit is zero. **Sort orders cost write IO; they should be earned by query patterns.**

### Break 3: too-large files

```python
# Push target file size to 2 GB:
# 'write.target-file-size-bytes' = '2147483648'

# Now a single-row update or delete rewrites 2 GB. Compaction takes hours.
# Query parallelism drops because there are fewer splits.
```

The sweet spot is 100–512 MB. Above 1 GB you start to lose parallelism; below 50 MB per-file overhead dominates.

### Break 4: hash distribution on a low-cardinality column

```python
# 'write.distribution-mode' = 'hash' partitioned by country
# 8 countries, 200 writer tasks. 200 tasks shuffle data, 8 of them write.
# 192 tasks do nothing. Wasted cluster time.
```

`hash` distribution wants partition cardinality ≥ writer-task count. If your partition cardinality is 8 but your cluster has 200 tasks, use `none` or `range`.

### Break 5: bloom filter on a low-cardinality column

```python
# Bloom filter on country (8 values)
# False-positive rate is meaningful only when value space is large.
# For low-cardinality columns, the bloom filter occupies space and helps nobody.
```

Bloom filters are worth it for cardinality > ~1000. Below that, file-level min/max stats already do the job.

---

## 📚 Theory deep-dive

### The diagnosis order

When a query is slow, check these in order — most common cause first:

1. **Too many files / manifests** (planning bottleneck)
   - Symptom: even a `SELECT COUNT(*)` is slow.
   - Diagnose: `len(inspect.files())`, `len(inspect.manifests())`.
   - Fix: `rewrite_data_files`, `rewrite_manifests`.

2. **No pruning at file level** (file selection bottleneck)
   - Symptom: `plan_files()` returns most files even with a selective predicate.
   - Diagnose: check sort order, partition spec, and the predicate columns' min/max in `.files`.
   - Fix: change sort order, add partition transform, enable bloom filters.

3. **Files too big or too small** (scan bottleneck)
   - Symptom: planning is fast, pruning is good, but scan time dominates.
   - Diagnose: file size distribution.
   - Fix: tune `write.target-file-size-bytes`, run sort-based compaction.

4. **Engine-side issues**
   - Symptom: Spark/Trino-specific behavior, plans look fine.
   - Diagnose: explain plan.
   - Fix: engine-side tuning (broadcast joins, dynamic partition pruning, etc.) — outside Iceberg's scope.

Walk these in order. Most engineers jump to step 4 ("Spark is slow") when the answer is step 1.

### The four knobs in detail

**Partition spec**
- Uses transforms: `identity`, `year/month/day/hour`, `bucket(N, col)`, `truncate(N, col)`.
- Target partition cardinality: 100–10,000.
- Target partition size: 100MB–10GB.
- Hidden partitioning: writers pass real column values; Iceberg computes transform on write. Queries don't need to know about transforms.

**Sort order**
- One-dimensional ordering within each file (and *across* files when range distribution is used).
- Helps file-level min/max pruning for filters on sort columns.
- Costs extra IO during compaction.
- Pick based on the dominant filter column.

**Target file size**
- `write.target-file-size-bytes`, default 512MB.
- 100MB–1GB is the production sweet spot.
- Smaller → more parallelism, more per-file overhead, more manifest entries.
- Larger → less parallelism, single-row mutations rewrite huge files.

**Write distribution mode**
- `write.distribution-mode = none | hash | range`.
- `none`: tasks write independently — many small files per partition.
- `hash`: shuffle by partition key — one file per (partition, task).
- `range`: globally order, then write — best query performance, highest write cost.

### Bloom filters

- Configured per column via table properties:
  ```
  write.parquet.bloom-filter-enabled.column.<col> = true
  write.parquet.bloom-filter-fpp.column.<col>     = 0.01
  write.parquet.bloom-filter-max-bytes            = 1048576
  ```
- Best for **high-cardinality columns** queried with **equality predicates** (`user_id = 12345`).
- Cost: extra bytes in Parquet footers (~1 MB per file by default).
- Not useful for: range queries (use sort order instead), low-cardinality columns (use min/max), or columns never filtered.

### When to *not* tune

A surprising number of "slow Iceberg tables" don't need Iceberg tuning at all:

- Your query is doing a 50TB join. No file format will save you.
- Your engine has a misconfigured shuffle. Fix the engine.
- Your network between compute and storage is the bottleneck. Move compute closer.

Run an **EXPLAIN** in your engine before touching the table. If the engine's plan is bad, fix that first.

### Tuning vs over-tuning

Every knob you turn has an ongoing cost — every future compaction has to honor it. Defaults are reasonable. Tune only when you have:

1. Measurements showing the problem
2. A hypothesis tied to a specific knob
3. Acceptance that the knob's cost is recurring, not one-time

If you're tuning more than two knobs at once, you're guessing.

---

## ✍️ Re-answer the interview question

> "An Iceberg table is slow. What knobs do you turn?"

Cover:

1. **Diagnosis order**: plan cost → file selection → scan cost → engine. Most engineers jump to step 4; senior engineers walk steps 1–4.
2. **Four knobs**: partition spec (cardinality 100–10000, use transforms), sort order (match dominant filter), file size (100MB–1GB), write distribution (none/hash/range — match cluster + cardinality).
3. **Bloom filters** as a tactical add for high-cardinality point lookups.
4. **When NOT to tune** — engine misconfig, network, query is fundamentally heavy.
5. **The cost** of every knob is *ongoing*. Default is reasonable; tune only with evidence.

---

## 🎁 LinkedIn post draft

> **"Most teams 'optimize' Iceberg tables by adding more partitions. That's usually wrong. Here's the diagnosis order I walk before touching a single table property."**
>
> Walk the four-step diagnosis. End with "the partition spec is the *last* knob you should turn, not the first — because it's the one with the most ongoing cost."

---

## Session wrap-up

1. Confirm hands-on: you have the three tables (`unsorted`, `sorted_by_ts`, `by_country`) with measured before/after timings; you've seen which queries are helped by which knob.
2. Save the `time_query()` helper — production debugging tool.
3. Update `interview-faq.md`.

---

## Next up

→ [Experiment 20: Multi-engine interop](20-multi-engine-interop.md) — your tuning needs to survive readers you don't control. Spark writes, Trino reads, DuckDB reads. What breaks?
