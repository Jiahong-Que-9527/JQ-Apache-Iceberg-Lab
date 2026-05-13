# Experiment 07 — Hidden Partitioning

> **Time:** 90 min · **Tier:** Mechanics · **Prerequisites:** Experiments 01–06

---

## 🎯 The interview question this answers

> **"What's hidden partitioning and why does it matter? Can you change a table's partitioning without rewriting all data?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** Hive's partitioning model has caused more data engineering bugs than any other single design choice in the last 15 years. If you can articulate what Iceberg fixed and why it matters, you sound like someone who has been bitten — and that's exactly the seniority signal interviewers look for.

---

## TL;DR

In Hive, you partition by extracting a derived value (e.g., `event_date`) and storing it as a separate column users must filter on. In Iceberg, the partition is **derived from a source column via a transform** (e.g., `days(event_ts)`), and users just filter on the natural column. The partition column is "hidden" — it exists in metadata, not in your SQL.

The killer feature: **you can change the partitioning scheme without rewriting old data**. Hive can't. This is what makes Iceberg viable for evolving production tables.

---

## 🛠️ Hands-on: feel both worlds

### Step 1 — The Hive way (the bad way)

In Hive, you'd write:

```sql
CREATE TABLE events_hive (
    event_id STRING,
    user_id BIGINT,
    amount DOUBLE,
    event_date STRING  -- derived from event_ts, stored separately
) PARTITIONED BY (event_date);
```

Users querying must:

```sql
-- WORKS, hits partition pruning
SELECT * FROM events_hive WHERE event_date = '2026-05-13';

-- ALSO HITS the right files BUT requires user to know the derivation
SELECT * FROM events_hive WHERE event_date = '2026-05-13' AND event_ts > ...;

-- DOES NOT hit partition pruning — full scan!
SELECT * FROM events_hive WHERE event_ts > '2026-05-13 10:00:00';
```

**The user has to know the partition scheme to query efficiently.** And worse: if they get it wrong, no warning — just slow queries.

### Step 2 — The Iceberg way (hidden partitioning)

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, BucketTransform
import pyarrow as pa
from datetime import datetime, timezone, timedelta

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=True),
    NestedField(3, "user_id", LongType()),
    NestedField(4, "amount", DoubleType()),
)

# Partition by day, derived from event_ts
partition_spec = PartitionSpec(
    PartitionField(
        source_id=2,                      # field-id of event_ts
        field_id=1000,                    # partition field id
        transform=DayTransform(),
        name="event_ts_day",              # for display, not for users to filter on
    )
)

table = catalog.create_table(
    "lab.events_iceberg",
    schema=schema,
    partition_spec=partition_spec,
)

print(table.spec())
```

### Step 3 — Write data across multiple days

```python
base_ts = datetime(2026, 5, 10, tzinfo=timezone.utc)
rows = []
for day in range(5):
    for hour in range(0, 24, 6):
        rows.append({
            "event_id": f"e_{day}_{hour}",
            "event_ts": base_ts + timedelta(days=day, hours=hour),
            "user_id": day * 100 + hour,
            "amount": 10.0 + day,
        })

data = pa.table({
    "event_id": [r["event_id"] for r in rows],
    "event_ts": [r["event_ts"] for r in rows],
    "user_id":  [r["user_id"] for r in rows],
    "amount":   [r["amount"] for r in rows],
})

table.append(data)
table = catalog.load_table("lab.events_iceberg")
```

### Step 4 — Look at the directory structure in MinIO

In MinIO console, browse to `warehouse/lab/events_iceberg/data/`. You should see folders like:

```
event_ts_day=2026-05-10/
event_ts_day=2026-05-11/
event_ts_day=2026-05-12/
event_ts_day=2026-05-13/
event_ts_day=2026-05-14/
```

Note: the partition value is human-readable (`2026-05-10`) but **it's not a column in your schema**. It exists only in the file path and in manifest metadata.

### Step 5 — Query using the source column directly

```python
# Notice: we filter on event_ts (the source column), NOT on event_ts_day
result = table.scan(
    row_filter="event_ts >= '2026-05-12T00:00:00' AND event_ts < '2026-05-13T00:00:00'"
).to_arrow()

print(result.to_pandas())
```

This works because Iceberg knows `event_ts_day = days(event_ts)`. It translates the filter on the source column into a partition predicate automatically.

**Compare to Hive:** in Hive, this filter would do a full scan. In Iceberg, it reads exactly one partition.

### Step 6 — Prove it: inspect the query plan

```python
plan = list(table.scan(
    row_filter="event_ts >= '2026-05-12T00:00:00' AND event_ts < '2026-05-13T00:00:00'"
).plan_files())

print(f"Files planned: {len(plan)}")
for f in plan:
    print(f"  {f.file.file_path}")
```

You should see **only files from `event_ts_day=2026-05-12`**. Iceberg pruned the rest at the manifest level. This is the payoff.

### Step 7 — Evolve the partition spec (the killer feature)

Six months later, daily partitions are too coarse — you want hourly:

```python
from pyiceberg.transforms import HourTransform

with table.update_spec() as us:
    us.add_field("event_ts", HourTransform(), partition_field_name="event_ts_hour")
    us.remove_field("event_ts_day")

table = catalog.load_table("lab.events_iceberg")
print("New spec:")
print(table.spec())
print("\nSpec history (kept for old data):")
print(table.specs())
```

Now write new data:

```python
new_data = pa.table({
    "event_id": ["new_1", "new_2"],
    "event_ts": [
        datetime(2026, 5, 15, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 15, 14, 0, tzinfo=timezone.utc),
    ],
    "user_id": [1, 2],
    "amount": [1.0, 2.0],
})
table.append(new_data)
table = catalog.load_table("lab.events_iceberg")
```

Look at MinIO again. You now have **two partition layouts coexisting**:

```
event_ts_day=2026-05-10/    ← old data, old spec
...
event_ts_day=2026-05-14/
event_ts_hour=2026-05-15-10/  ← new data, new spec
event_ts_hour=2026-05-15-14/
```

**Old data was not rewritten.** It still uses the old partitioning. New data uses the new partitioning. The query engine handles both transparently.

### Step 8 — Verify queries still work across the boundary

```python
# Query spanning both old and new partitioning
result = table.scan(
    row_filter="event_ts >= '2026-05-13T00:00:00' AND event_ts <= '2026-05-15T23:00:00'"
).to_arrow()

print(result.to_pandas())
```

Iceberg uses the old spec for files written before the change, the new spec for files written after. **One query, two physical layouts, correct results.** This is the operational killer feature.

---

## 💥 Break it

### Break 1: try to filter on the partition column

```python
# event_ts_day is NOT a column — it's a partition field
try:
    table.scan(row_filter="event_ts_day = '2026-05-12'").to_arrow()
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

You cannot reference the partition field name in a filter. It's hidden by design — you filter on the source column.

### Break 2: high-cardinality partitioning disaster

```python
# Create a table partitioned by user_id directly — bad idea
schema2 = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "user_id", LongType(), required=True),
    NestedField(3, "amount", DoubleType()),
)

from pyiceberg.transforms import IdentityTransform

bad_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="user_id"),
)

bad_table = catalog.create_table("lab.bad_partition", schema=schema2, partition_spec=bad_spec)

# Write data with 10000 distinct user_ids
import numpy as np
data = pa.table({
    "event_id": [f"e{i}" for i in range(10000)],
    "user_id": list(range(10000)),
    "amount": np.random.random(10000).tolist(),
})
bad_table.append(data)
```

Now look at MinIO. You should see **10,000 tiny Parquet files**, one per user_id. **This is a small-files disaster** (explored fully in exp09). 

**Insight:** partitioning is not free. Too many partitions = too many small files = slow queries. Rule of thumb: aim for partitions containing at least 100MB–1GB of data each.

### Break 3: bucket transform for high cardinality

The right way to handle high-cardinality keys: **bucketing**.

```python
better_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=BucketTransform(num_buckets=16), name="user_id_bucket"),
)

better_table = catalog.create_table("lab.bucketed", schema=schema2, partition_spec=better_spec)
better_table.append(data)
```

Now check MinIO: **16 buckets, ~625 rows each**. Manageable file count, still benefits from partition pruning when querying specific user_ids.

```python
# Iceberg can use the bucket transform for pruning
plan = list(better_table.scan(row_filter="user_id = 42").plan_files())
print(f"Files for user_id=42: {len(plan)} out of 16 buckets")
# Should print 1 — only one bucket contains user_id=42
```

---

## 📚 Theory deep-dive

### The supported transforms

Iceberg's spec defines these partition transforms:

| Transform | Use for | Example |
|---|---|---|
| `identity` | Low-cardinality categorical | `country`, `status` |
| `bucket(N)` | High-cardinality identifiers | `user_id`, `order_id` |
| `truncate(L)` | String prefixes, integer ranges | `name` → first 3 chars |
| `year` | Annual partitioning | `created_at` → year |
| `month` | Monthly partitioning | `created_at` → month |
| `day` | Daily partitioning (most common) | `event_ts` → day |
| `hour` | Hourly partitioning | `event_ts` → hour |

**The transform determines how the engine can prune.** A query `WHERE event_ts > '2026-05-13 10:30:00'` will read the `2026-05-13` partition (day transform) or `2026-05-13-10` partition (hour transform), respectively.

### Why hidden partitioning is better

The Hive model has three failure modes that Iceberg fixes:

| Failure mode | Hive | Iceberg |
|---|---|---|
| User forgets to filter on partition column | Full scan, silent slowness | Pruning happens regardless |
| User writes wrong partition value | Data lost / queryable but wrong | Iceberg derives, no manual error |
| Partition scheme is wrong for workload | Must rewrite entire table | Evolve spec, old data untouched |

### How to choose a partitioning scheme

Decision framework:

1. **What queries dominate?** Filter on time? Filter on entity ID? Both?
2. **What's the data volume per day?** This determines partition size.
3. **What's the file count target?** Aim for 100MB–1GB per file, 10–100 files per partition.
4. **What's the cardinality of candidate partition keys?** Identity on high-cardinality = disaster.
5. **Will the query pattern change?** Easier to evolve in Iceberg than Hive, but still plan ahead.

Rough rules:
- **Time-series data, time-based queries dominant** → day or hour transform on the timestamp
- **Entity-based data with point lookups by ID** → bucket transform on the ID
- **Mixed workload** → multi-field partitioning (e.g., day + bucket)
- **Small tables (< 100GB total)** → consider not partitioning at all

### The "I partitioned by user_id" anti-pattern

This is the single most common partitioning bug in the wild. People reason: "users query by user_id, so I'll partition by user_id." Result: one folder per user. If you have 1M users, you have 1M folders, each tiny. **Performance disaster.**

Fix: bucket on user_id. The query engine still prunes to ~1/N of the data, but you have N folders total (typically N=16 or 32), and each contains a healthy chunk.

**Knowing this anti-pattern by name is a strong interview signal.**

### Multi-field partitioning

```python
spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="event_day"),
    PartitionField(source_id=3, field_id=1001, transform=BucketTransform(16), name="user_bucket"),
)
```

This creates a hierarchical layout: `event_day=X/user_bucket=Y/`. Queries can prune on either or both. Good for **time-series with hot users** workloads.

Don't over-partition. Two fields is usually enough. Three is rare and usually wrong.

---

## 🇪🇺 Regulatory angle

Partition evolution is **directly relevant to data retention policies**.

**GDPR Article 5(1)(e)** requires data not be kept longer than necessary. **DORA** has similar themes. In practice this means: delete data older than X.

In Hive: deleting old data means rewriting the table (slow, expensive, risky during execution).
In Iceberg with day-partitioning: deleting old data is metadata-only at the partition level. Snapshots make it auditable. The combination is exactly what an EU compliance team wants.

Specifically: with daily partitioning, "delete everything older than 7 years" becomes a partition-by-partition operation that's predictable, auditable, and reversible (via snapshot rollback) if you make a mistake.

**In interviews:** "We use daily hidden partitioning specifically because it aligns with GDPR/DORA retention enforcement. Each daily partition is its own deletable unit, and the snapshot history gives us the audit trail."

---

## ✍️ Re-answer the interview question

> "What's hidden partitioning, and can you change partitioning without rewriting data?"

Cover:

1. Hive: partition column = separate column users must filter on. Iceberg: partition = derived from source column via transform.
2. Hidden = users filter on natural column; Iceberg handles the translation.
3. Supported transforms: identity, bucket, truncate, year/month/day/hour.
4. Partition spec evolution: change the spec, old data keeps old layout, new data uses new layout, queries work across the boundary.
5. Anti-pattern: identity on high-cardinality. Fix: bucket transform.

---

## 🎁 LinkedIn post draft

> **"Hive partitioning is the worst design decision in modern data engineering history. Here's why — and how Iceberg fixed it."**
>
> Strong hook. Walk through the three failure modes table. Engineers who've been burned by Hive will recognize themselves in this post and engage heavily.

---

## Next up

→ [Experiment 08: Concurrent writes & isolation](08-concurrent-writes.md) — two writers, one table. What actually happens?
