# Experiment 04 — Reads Without Spark

> **Time:** 60 min · **Tier:** Foundation · **Prerequisites:** Experiments 01, 02

---

## 🎯 The interview question this answers

> **"How would you query Iceberg from a Python service or a non-JVM application? When would you need Spark anyway?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** This question is increasingly common as teams shift from "everything is Spark" to "use the right tool." Showing you've thought about the JVM-free read path is a marker of someone who keeps up with the ecosystem. Bonus: it directly maps to building lightweight microservices on top of a lakehouse.

---

## TL;DR

You can read Iceberg from at least four non-Spark paths in 2026: **PyIceberg + PyArrow**, **DuckDB**, **Trino/StarRocks**, and **Arrow Flight via REST**. They have different latency, memory, and feature profiles. Today you'll benchmark three of them on the same query.

---

## 🛠️ Hands-on: three reads, three latencies

### Step 1 — Generate a realistic test table

Reset and create a bigger table:

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType, DoubleType
import pyarrow as pa
import numpy as np
from datetime import datetime, timezone, timedelta

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "txn_id", StringType(), required=True),
    NestedField(2, "txn_ts", TimestampType(), required=True),
    NestedField(3, "user_id", LongType()),
    NestedField(4, "merchant_id", LongType()),
    NestedField(5, "amount_eur", DoubleType()),
    NestedField(6, "country", StringType()),
)

table = catalog.create_table("lab.transactions", schema=schema)

# Generate 1 million rows across 30 days
np.random.seed(42)
N = 1_000_000
base_ts = datetime(2026, 4, 13, tzinfo=timezone.utc)
data = pa.table({
    "txn_id": [f"t{i}" for i in range(N)],
    "txn_ts": [base_ts + timedelta(seconds=int(s)) for s in np.random.randint(0, 30*24*3600, N)],
    "user_id": np.random.randint(1, 100000, N).tolist(),
    "merchant_id": np.random.randint(1, 5000, N).tolist(),
    "amount_eur": np.round(np.random.gamma(2.0, 30.0, N), 2).tolist(),
    "country": np.random.choice(["DE", "NL", "FR", "IT", "ES"], N).tolist(),
})

table.append(data)
print(f"Wrote {N:,} rows.")
```

### Step 2 — Path A: PyIceberg + PyArrow

```python
import time

table = catalog.load_table("lab.transactions")

t0 = time.time()
result = (
    table.scan(
        row_filter="country == 'DE' AND amount_eur > 100",
        selected_fields=("txn_id", "amount_eur", "country"),
    )
    .to_arrow()
)
t1 = time.time()
print(f"PyIceberg: {len(result):,} rows in {(t1-t0)*1000:.0f} ms")
```

**What's happening:**
- PyIceberg does the entire query plan in Python (catalog → metadata → manifests → file pruning)
- Final read uses PyArrow's Parquet reader
- **No SQL engine**. Just file IO + filter pushdown.

### Step 3 — Path B: DuckDB

```python
import duckdb, os

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='{os.environ["AWS_ACCESS_KEY_ID"]}';
    SET s3_secret_access_key='{os.environ["AWS_SECRET_ACCESS_KEY"]}';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
""")

t0 = time.time()
result = con.execute(f"""
    SELECT txn_id, amount_eur, country
    FROM iceberg_scan('{table.location()}')
    WHERE country = 'DE' AND amount_eur > 100
""").fetchdf()
t1 = time.time()
print(f"DuckDB:    {len(result):,} rows in {(t1-t0)*1000:.0f} ms")
```

DuckDB's Iceberg extension reads metadata itself, then uses its vectorized SQL engine.

### Step 4 — Path C: Polars (if installed)

```python
# pip install polars deltalake-iceberg-rust 2026-style equivalent — check current docs
# This is optional. Skip if not installed.

import polars as pl

t0 = time.time()
result = (
    pl.scan_iceberg(table.metadata_location)
    .filter((pl.col("country") == "DE") & (pl.col("amount_eur") > 100))
    .select(["txn_id", "amount_eur", "country"])
    .collect()
)
t1 = time.time()
print(f"Polars:    {len(result):,} rows in {(t1-t0)*1000:.0f} ms")
```

### Step 5 — Compare

Run each at least 3 times (warm cache) and write down the numbers. You should see:

- PyIceberg: slowest for analytics (no vectorized engine, plain PyArrow filter)
- DuckDB: fastest, often by 5-10x
- Polars: comparable to DuckDB or faster on lazy plans

**Insight:** "read Iceberg" doesn't mean "use Spark." For analytics-style queries on tables up to ~100GB, DuckDB embedded in a Python process is **faster than Spark on the same hardware**, because there's no JVM startup, no shuffle, no executor overhead.

---

## 💥 Break it

### Break 1: query a column that doesn't exist

```python
try:
    table.scan(row_filter="nonexistent_col == 1").to_arrow()
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

What error do you get? PyIceberg validates references against the schema **before** issuing any S3 reads. You get a clear error instead of a 500ms hit to S3 just to fail. **Insight:** schema validation at plan time is one of Iceberg's UX wins over raw Parquet.

### Break 2: turn off pushdown and see what happens

Force a full table scan in DuckDB:

```python
t0 = time.time()
result = con.execute(f"""
    SELECT *
    FROM iceberg_scan('{table.location()}')
""").fetchdf()
t1 = time.time()
print(f"Full scan: {len(result):,} rows in {(t1-t0)*1000:.0f} ms, returned size: {result.memory_usage().sum()/1e6:.1f} MB")
```

Compare to the filtered version. The filtered query is much faster **and** uses much less memory. **Insight:** Iceberg's value is most visible when you have predicates that prune work.

### Break 3: simulate slow object storage

If you're on a fast local MinIO, you won't see realistic S3 latency. To simulate it:

```python
# Add artificial latency by querying many small files
# (First make sure to write data in many small batches in setup)

# Or: write 100 single-row appends instead of one 1M-row batch,
# then time queries. The metadata overhead becomes visible.
```

This previews Experiment 09 (small files problem).

---

## 📚 Theory deep-dive

### When you actually need Spark

People often assume "lakehouse = Spark." That hasn't been true since 2023. The honest decision tree:

```
Does the workload involve...

  ...analytics queries on tables < 100GB?
    → DuckDB / Polars / PyIceberg. No Spark needed.

  ...large ETL with shuffles / joins between huge tables?
    → Spark (or Trino, depending on architecture).

  ...streaming ingestion?
    → Spark Structured Streaming, Flink, or Kafka Connect Iceberg sink.

  ...row-level merges / UPSERTs on multi-TB tables?
    → Spark (still the most mature for MERGE INTO).

  ...interactive SQL with strict latency SLAs?
    → Trino or StarRocks (designed for this).

  ...ML feature engineering at scale?
    → Spark or Ray, depending on the team's preference.

  ...ad-hoc analytics in a notebook?
    → DuckDB. Period.
```

### The "warehouse on a laptop" pattern

DuckDB + PyIceberg + S3-compatible storage is a real production pattern for:

- Internal analytics tools where users self-serve queries
- BI engines that need to query lakehouse data directly
- Microservices that need read-only access to a single table or two
- Reproducible ML training pipelines (read a specific snapshot)

For these workloads, **introducing Spark adds operational cost without performance benefit**. Many teams that adopted Spark in 2020 are now removing it from these workloads.

**Interview signal:** if you can articulate the above tradeoffs cleanly, you're showing you understand the ecosystem dynamics, not just the tool.

### The Arrow connection

Notice that all three paths (PyIceberg, DuckDB, Polars) speak Arrow natively. This is not a coincidence — Arrow is becoming the **lingua franca for analytics in-memory**. Iceberg's PyArrow integration means you can pass data between engines without serialization cost.

This is why "modern data stack" architectures look like:

```
S3/MinIO (Parquet)
  ↓
Iceberg metadata (manifest pruning)
  ↓
Arrow (zero-copy in-memory)
  ↓
DuckDB / Polars / Pandas / your ML framework
```

Compare to legacy:

```
HDFS (Parquet)
  ↓
Spark (JVM, copies, serialization)
  ↓
Pandas (out of Spark, more copies)
```

**The Arrow story is a great topic to bring up unprompted in interviews.** It shows you understand where the industry is going.

---

## 🇪🇺 Regulatory angle

For EU FinTech: the JVM-free read path has a specific compliance benefit — **smaller attack surface**.

A Python service that reads Iceberg directly:
- One process, one language, one set of dependencies
- No JVM CVE history to manage
- Easier to verify and audit

A Spark service:
- JVM + Hadoop + native libraries + Python (PySpark)
- Much larger CVE surface
- More dependencies to track for **DORA Article 9** (ICT third-party risk)

**This is not theoretical.** Several large EU banks have explicit policies discouraging JVM-based services for new builds. If you're interviewing at one and the topic comes up, mentioning this shows operational awareness.

---

## ✍️ Re-answer the interview question

> "How would you query Iceberg from a Python service? When would you need Spark anyway?"

Cover:

1. Three concrete read paths: PyIceberg, DuckDB, Polars (and Trino as a fourth)
2. When each is best: DuckDB for analytics, PyIceberg for service integration, Trino for multi-tenant SQL
3. When you actually need Spark: shuffles, MERGE INTO, streaming, multi-TB joins
4. Modern architecture pattern: Arrow as the in-memory protocol

---

## 🎁 LinkedIn post draft

> **"I queried a 1M-row Iceberg table three ways: PyIceberg, DuckDB, Polars. Spark was nowhere in sight. Here's what I learned."**
>
> Include your benchmark numbers. End with: "The lakehouse used to mean Spark. In 2026, it means Arrow."

This is a high-engagement angle. The "DuckDB vs Spark for analytics" discourse is loud in the data community right now.

---

## Foundation tier complete ✅

You've now completed Tier 1. At this point you should be able to:

- Explain the four-layer Iceberg architecture from memory
- Walk through a query plan file-by-file
- Compare catalog implementations and recommend one
- Choose the right engine for a given read workload

**Checkpoint:** before moving to Tier 2, re-answer interview questions 1–6 in your `interview-faq.md` without looking at notes. If you can't, redo the experiment that covered the gap.

---

## Next up

→ [Experiment 05: Snapshots & time travel](05-snapshots-time-travel.md) — we move into Tier 2 (Mechanics). Snapshots, ACID, time travel, rollback, branching, tags. This is where Iceberg starts to feel magical.
