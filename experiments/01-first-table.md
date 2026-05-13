# Experiment 01 — Your First Iceberg Table

> **Time:** 60–90 min · **Tier:** Foundation · **Prerequisites:** `iceberg-lab` running, `00_setup_check` passed

---

## 🎯 The interview question this answers

> **"What does an Iceberg table physically consist of?"**

Pause. Open your `interview-faq.md`. Write your current answer in 2–3 sentences. Even if it's wrong. Especially if it's wrong.

**Why this matters:** This is the #1 most common opening question for any Iceberg-related interview. If you can't answer it crisply in 60 seconds, the interviewer will assume you've only used Iceberg through SQL and never understood it. Game over for senior roles.

---

## TL;DR (read this last, not first)

An Iceberg table is **not** "a folder full of Parquet files." It's a **tree of metadata pointers** that happens to point to Parquet (or ORC, or Avro) at the leaves. The metadata tree is what makes Iceberg's ACID, time travel, and schema evolution possible. Today you'll create that tree and look at it.

---

## 🛠️ Hands-on: build it

### Step 1 — Wake up the lab

```bash
cd iceberg-lab
./init.sh
# open http://localhost:8888
```

Create a new notebook called `exp01-first-table.ipynb`. Or use the prepared one in `notebooks/01_basics.ipynb`.

### Step 2 — Get a catalog

```python
from src.catalog_helper import get_catalog

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")
```

**Pause and think:** what did `get_catalog()` give you? Is it a connection to a database? A file handle? A REST client?

Run this:

```python
print(type(catalog))
print(catalog.uri)
print(catalog.properties)
```

You should see it's a `SqlCatalog` backed by `sqlite:///catalog.db`. **This is your first insight:** the catalog is just a database. Right now it's SQLite. In production it might be Postgres, Glue, Nessie, Polaris — but the API is the same.

### Step 3 — Define a schema (the Iceberg way)

```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, TimestampType, DoubleType, StructType
)

schema = Schema(
    NestedField(field_id=1, name="event_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="event_ts", field_type=TimestampType(), required=True),
    NestedField(field_id=3, name="user_id", field_type=LongType(), required=False),
    NestedField(field_id=4, name="amount", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="location", field_type=StructType(
        NestedField(field_id=6, name="country", field_type=StringType()),
        NestedField(field_id=7, name="city", field_type=StringType()),
    ), required=False),
)
```

**Notice the `field_id`s.** Every field has a unique numeric ID. Remember this. It's the single most important design decision in the entire Iceberg spec. We'll see why in experiment 06.

### Step 4 — Create the table

```python
table = catalog.create_table(
    identifier="lab.events",
    schema=schema,
)

print(table.location())
```

The location will be something like `s3://warehouse/lab/events`. **Open the MinIO console** at http://localhost:9001 (login: minioadmin / minioadmin). Click `warehouse` bucket → `lab/events/`.

What do you see?

You should see **only a `metadata/` folder**, and inside it a single file: `v1.metadata.json`. **There is no data folder yet.** You just created a table, and zero Parquet files exist. **This is the second insight:** Iceberg tables are defined by metadata, not by data. A table with zero rows is still a table — because its metadata exists.

### Step 5 — Write some data

```python
import pyarrow as pa
from datetime import datetime, timezone

data = pa.table({
    "event_id":  ["e1", "e2", "e3"],
    "event_ts":  [datetime(2026, 5, 13, 10, 0, tzinfo=timezone.utc),
                  datetime(2026, 5, 13, 10, 5, tzinfo=timezone.utc),
                  datetime(2026, 5, 13, 10, 10, tzinfo=timezone.utc)],
    "user_id":   [101, 102, 101],
    "amount":    [9.99, 19.99, 4.50],
    "location":  [{"country": "DE", "city": "Frankfurt"},
                  {"country": "DE", "city": "Berlin"},
                  {"country": "NL", "city": "Amsterdam"}],
})

table.append(data)
```

**Now go back to the MinIO console.** Refresh. You should see:

```
warehouse/lab/events/
├── data/
│   └── 00000-0-<uuid>.parquet      ← your actual data
└── metadata/
    ├── v1.metadata.json
    ├── v2.metadata.json            ← NEW: a new version
    ├── snap-<id>-<id>.avro          ← NEW: the manifest list
    └── <id>-m0.avro                 ← NEW: the manifest file
```

**Count them: one append, four new files.** This is the third insight: **every write produces metadata files, not just data files**. This is the price of ACID.

### Step 6 — Read it back

```python
# Refresh the table to get the latest snapshot
table = catalog.load_table("lab.events")

# Read as PyArrow
arrow_table = table.scan().to_arrow()
print(arrow_table.to_pandas())
```

Now try with DuckDB:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
""")
result = con.execute(f"""
    SELECT * FROM iceberg_scan('{table.location()}')
""").fetchdf()
print(result)
```

Same data, two different engines, **no Spark, no JVM**. Bookmark this — it's directly relevant to interview question #6.

---

## 💥 Break it (the most important section)

This is where most tutorials stop. Don't.

### Break 1: delete a data file directly

In the MinIO console, navigate to `lab/events/data/` and **delete the Parquet file** (right-click → Delete).

Now in your notebook:

```python
table = catalog.load_table("lab.events")
try:
    print(table.scan().to_arrow())
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

What happened? The table thinks it still has 3 rows (because the metadata says so), but reading them fails. **Insight:** Iceberg trusts its metadata. If you bypass Iceberg and modify files directly, you break the contract.

**Reset:** `./reset.sh` and re-run steps 2–5 to get back to a good state.

### Break 2: delete a metadata file

After resetting and re-running through step 5, this time go to `metadata/` and **delete `v2.metadata.json`** (keep `v1.metadata.json`).

```python
table = catalog.load_table("lab.events")
print(table.scan().to_arrow())
```

What happened?

The table still works — but it might show **zero rows**, because the catalog's pointer was updated, but if you query at the right moment... actually try this:

```python
# Check what version the catalog thinks is current
print(table.metadata_location)
```

The catalog still points to `v2.metadata.json`, which no longer exists. **Insight:** the catalog's job is to hold a single pointer: "the current metadata location." If you break this pointer, the table is gone.

**Reset and continue.**

### Break 3: try to read with the wrong credentials

```python
import duckdb
con = duckdb.connect()
con.execute("SET s3_access_key_id='wrong';")
con.execute(f"SELECT * FROM iceberg_scan('{table.location()}')")
```

What error do you get? Note it. In production, 60% of "Iceberg doesn't work" tickets are S3 auth issues. Recognize the error message now and save yourself debugging time later.

---

## 📚 Theory deep-dive (now that you've felt it)

### The four-layer model

```
┌─────────────────────────────────────────────┐
│ 1. CATALOG                                  │
│    "Where is the current metadata file?"    │
│    SQLite/Postgres/Glue/Nessie/...          │
└────────────────────┬────────────────────────┘
                     │ points to
                     ▼
┌─────────────────────────────────────────────┐
│ 2. METADATA FILE  (v{N}.metadata.json)      │
│    Schema, partition specs, snapshot list   │
│    Points to the current snapshot           │
└────────────────────┬────────────────────────┘
                     │ snapshot points to
                     ▼
┌─────────────────────────────────────────────┐
│ 3. MANIFEST LIST  (snap-*.avro)             │
│    List of manifests + partition summary    │
│    Used for fast partition pruning          │
└────────────────────┬────────────────────────┘
                     │ list of
                     ▼
┌─────────────────────────────────────────────┐
│ 4. MANIFESTS  (*.avro)                      │
│    List of data files + column-level stats  │
│    Used for fast file pruning               │
└────────────────────┬────────────────────────┘
                     │ list of
                     ▼
              [Parquet data files]
```

Memorize this. In any interview, if you can draw this diagram on a whiteboard while explaining each layer, you've already passed the bar for "understands Iceberg fundamentals."

### Why three layers of metadata instead of one?

This is a common follow-up question. The honest answer is: **performance**.

- A query like `SELECT * WHERE country='DE' AND date='2026-05-13'` does **not** want to open every Parquet file to check.
- The **manifest list** lets it skip entire manifests based on partition summaries.
- The **manifests** let it skip individual files based on column min/max statistics.
- Only **after** pruning at both levels does the query touch actual Parquet files.

This is called **pushdown predicate evaluation**, and it's why Iceberg beats Hive on query planning time by 10-100x for large tables.

### What about the catalog?

The catalog's job is **atomic pointer swap**. When you commit a new snapshot:

1. Iceberg writes new data files (Parquet) to S3
2. Iceberg writes a new manifest, new manifest list, and new `v{N+1}.metadata.json`
3. The catalog atomically updates its pointer from `v{N}.metadata.json` to `v{N+1}.metadata.json`

If step 3 fails (e.g., another writer raced you), Iceberg retries — but **no partial state is ever visible to readers**. This is how Iceberg gets ACID without locking.

We'll see this in action in Experiment 08.

---

## 🇪🇺 Regulatory angle (optional)

For EU FinTech roles: the four-layer model is **directly relevant to DORA Article 28** (ICT third-party risk, data portability). Because every layer is a documented open spec (Iceberg Table Spec v2), regulators can verify that your data is not locked into a proprietary format. Compare this to a hypothetical "Snowflake-only" table — extracting it in an emergency requires Snowflake's cooperation. Iceberg + S3 means **your data is operable by any compliant engine**, including ones a regulator could spin up themselves.

When asked "why Iceberg in a regulated environment?" — this is the single most powerful answer.

---

## ✍️ Re-answer the interview question

Go back to your `interview-faq.md`. Find your original answer to:

> "What does an Iceberg table physically consist of?"

Now write the new version. Aim for **60–90 seconds spoken**. Cover:

1. Catalog → metadata file → manifest list → manifests → data files (the four layers)
2. Why each layer exists (catalog = atomic pointer, manifests = file pruning)
3. The implication: an Iceberg table is its metadata. Data files alone are meaningless without it.

Read both versions. The delta is what this experiment taught you.

---

## 🎁 LinkedIn post draft (optional)

If you want to publish:

> **"I built my first Iceberg table today and discovered something I didn't expect: it doesn't actually contain data."**
>
> A 250-word post about the metadata-first design and what it means for ACID and time travel. End with: "Next up: walking through the manifest list byte-by-byte."

This is genuinely useful content for the EU data engineering community, and it builds your name in the space well before any recruiter searches for you.

---

## Next up

→ [Experiment 02: Anatomy of metadata](02-metadata-anatomy.md) — we open every metadata file by hand and explain every field. This is the highest-value experiment in the entire series for interviews.
