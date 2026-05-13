# Experiment 02 — Anatomy of Metadata

> **Time:** 120 min · **Tier:** Foundation · **Prerequisites:** Experiment 01
>
> ⭐ **This is the highest-value experiment in the series for interviews.** Do not skip the hands-on. Reading is not enough.

---

## 🎯 The interview question this answers

> **"Walk me through what happens when you query an Iceberg table. What files get read, in what order, and why?"**

Write your current answer in `interview-faq.md`. Be honest about gaps.

**Why this matters:** This question separates "I've used Iceberg" candidates from "I understand Iceberg" candidates. The former gets €60–75k. The latter gets €90k+. The entire difference is whether you can describe the file-by-file query plan from memory.

---

## TL;DR

A query against an Iceberg table opens up to **four kinds of files** in a specific order: catalog lookup → metadata.json → manifest list → manifests → data files. At each step, Iceberg prunes the work needed for the next step. Today you'll download each of these files from MinIO and open them with your own eyes.

---

## 🛠️ Hands-on: dissect the metadata tree

### Setup

Make sure you've completed Experiment 01 — you should have a `lab.events` table with some data. If not:

```bash
./reset.sh
# then run through experiment 01 again
```

Create `exp02-metadata-anatomy.ipynb`.

### Step 1 — Find the current metadata file

```python
from src.catalog_helper import get_catalog

catalog = get_catalog()
table = catalog.load_table("lab.events")

print("Metadata location:", table.metadata_location)
print("Table location:   ", table.location())
```

Note the `metadata_location` — it's a specific `v{N}.metadata.json`. **The catalog's only job is to know this one path.** Everything else can be reconstructed by reading this file.

### Step 2 — Download metadata.json and look at it

```python
import boto3
import json
import os

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

# parse the s3://... URL
def parse_s3(uri):
    path = uri.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key

bucket, key = parse_s3(table.metadata_location)
obj = s3.get_object(Bucket=bucket, Key=key)
metadata = json.loads(obj["Body"].read())

print(json.dumps(metadata, indent=2, default=str))
```

**Stop and read it.** All of it. Don't scroll past. Yes it's long.

### Step 3 — Walk through every top-level key

Here's what you should be seeing, with annotations:

```jsonc
{
  "format-version": 2,
  // The Iceberg spec version. v2 added row-level deletes (merge-on-read).
  // Most production tables in 2026 are v2. v3 is in draft.

  "table-uuid": "abc-123-...",
  // Globally unique. Survives table renames. Useful for audit trails.

  "location": "s3://warehouse/lab/events",
  // Root path. All data and metadata live under here.

  "last-sequence-number": 1,
  // Monotonically increasing. Used for MVCC and conflict detection.

  "last-updated-ms": 1715000000000,

  "last-column-id": 7,
  // Tracks the highest field-id ever assigned (see exp 06)

  "schemas": [
    {
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "event_id", "required": true, "type": "string"},
        {"id": 2, "name": "event_ts", ...},
        // ...
      ]
    }
  ],
  "current-schema-id": 0,
  // Schemas is a LIST. Old schemas are kept. This is what makes
  // schema evolution safe (exp 06).

  "partition-specs": [...],
  "default-spec-id": 0,
  // Same idea: partition specs are versioned. You can change
  // partitioning without rewriting data (exp 07).

  "sort-orders": [...],
  "default-sort-order-id": 0,

  "properties": {...},

  "current-snapshot-id": 1234567890,
  "snapshots": [
    {
      "snapshot-id": 1234567890,
      "timestamp-ms": ...,
      "summary": {
        "operation": "append",
        "added-data-files": "1",
        "added-records": "3",
        ...
      },
      "manifest-list": "s3://warehouse/lab/events/metadata/snap-...-1234.avro",
      "schema-id": 0
    }
  ],
  // The LIST OF SNAPSHOTS is your time travel history.
  // Each snapshot points to ONE manifest list.

  "snapshot-log": [...],
  "metadata-log": [...],
  // Audit trails.

  "refs": {
    "main": {"snapshot-id": 1234567890, "type": "branch"}
  }
  // Branches and tags (exp 05).
}
```

**Run this exercise:** print each top-level key separately and describe in your own words what it does. If you can't, you don't understand it yet.

```python
for key in metadata.keys():
    value = metadata[key]
    if isinstance(value, (list, dict)):
        print(f"{key}: <{type(value).__name__} of length {len(value)}>")
    else:
        print(f"{key}: {value}")
```

### Step 4 — Follow the snapshot pointer

```python
current_snapshot_id = metadata["current-snapshot-id"]
current_snapshot = next(s for s in metadata["snapshots"]
                        if s["snapshot-id"] == current_snapshot_id)

print("Current snapshot:")
print(json.dumps(current_snapshot, indent=2, default=str))

manifest_list_path = current_snapshot["manifest-list"]
print("\nManifest list location:", manifest_list_path)
```

This is the second hop: **metadata.json → snapshot → manifest list**.

### Step 5 — Open the manifest list (Avro)

The manifest list is an Avro file, not JSON. Install fastavro if needed:

```python
!pip install fastavro --quiet

import fastavro
import io

bucket, key = parse_s3(manifest_list_path)
obj = s3.get_object(Bucket=bucket, Key=key)
buffer = io.BytesIO(obj["Body"].read())

records = list(fastavro.reader(buffer))
print(f"Manifest list contains {len(records)} manifest(s).")
for r in records:
    print(json.dumps(r, indent=2, default=str))
```

**What you should see:** one record per manifest file. Each record has:

- `manifest_path`: where the manifest file is
- `manifest_length`: size in bytes
- `partition_spec_id`: which partition spec this manifest uses
- `added_files_count`, `existing_files_count`, `deleted_files_count`
- `added_rows_count`, `existing_rows_count`, `deleted_rows_count`
- `partitions`: partition summary — **this is what enables partition pruning at the manifest-list level**

**Key insight:** the manifest list lets a query engine answer "do any manifests in this snapshot contain data for partition X?" without opening any manifest files. For a table with 10,000 manifests, this saves 9,999 file opens.

### Step 6 — Open a manifest (Avro)

```python
manifest_path = records[0]["manifest_path"]
bucket, key = parse_s3(manifest_path)
obj = s3.get_object(Bucket=bucket, Key=key)
buffer = io.BytesIO(obj["Body"].read())

manifest_records = list(fastavro.reader(buffer))
print(f"Manifest contains {len(manifest_records)} entry(ies).")
for r in manifest_records:
    print(json.dumps(r, indent=2, default=str))
```

Each entry has a `data_file` sub-record with:

- `file_path`: the Parquet file location
- `file_format`: "PARQUET" (or ORC, AVRO)
- `record_count`
- `file_size_in_bytes`
- `column_sizes`: bytes per column
- `value_counts`, `null_value_counts`, `nan_value_counts`
- **`lower_bounds` and `upper_bounds`**: per-column min/max
- `partition`: the partition values for this file

**The min/max bounds are the magic.** A query like `WHERE amount > 10` can look at every file's upper_bound for `amount` and skip files where `upper_bound <= 10`, without opening them.

### Step 7 — Reconstruct the query plan

Now answer this with code:

```python
# Question: which Parquet files would a query like
#   SELECT * FROM lab.events WHERE amount > 10
# need to actually read?

# 1. Start from the catalog → metadata.json (we did this)
# 2. From metadata.json, find current snapshot → manifest list (we did this)
# 3. From manifest list, would partition summaries help? In our case no — we haven't partitioned by amount. So all manifests are candidates.
# 4. From each manifest, check each data file's upper_bound for `amount` column.

# Field ID for "amount" is 4 (from the schema)
target_field_id = 4
threshold = 10.0

for entry in manifest_records:
    df = entry["data_file"]
    # upper_bounds is a list of {key: field_id, value: bytes-encoded value}
    upper_bounds = {b["key"]: b["value"] for b in df["upper_bounds"]}

    if target_field_id in upper_bounds:
        # The value is stored as bytes — for a double, it's little-endian 8 bytes
        import struct
        max_val = struct.unpack("<d", upper_bounds[target_field_id])[0]
        keep = max_val > threshold
        print(f"File {df['file_path']}: max amount = {max_val}, keep = {keep}")
```

**You just manually executed predicate pushdown.** This is what Iceberg does internally for every query.

---

## 💥 Break it

### Break 1: corrupt a manifest

After you've inspected everything, try corrupting a manifest by deleting it from MinIO (don't delete the data file, just the `.avro` manifest):

```python
bucket, key = parse_s3(manifest_path)
s3.delete_object(Bucket=bucket, Key=key)

# now try to read
table = catalog.load_table("lab.events")
try:
    print(table.scan().to_arrow())
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

You'll see the read fail. **Insight:** manifests are not optional. The data files still exist, but Iceberg cannot find them without the manifest. This is why backups must include `metadata/`, not just `data/`.

Reset before continuing.

### Break 2: write more data, see metadata grow

```python
# After reset and re-running exp01, append two more batches
import pyarrow as pa
from datetime import datetime, timezone

for i in range(3):
    data = pa.table({
        "event_id": [f"e_batch{i}_{j}" for j in range(3)],
        "event_ts": [datetime(2026, 5, 13, 11+i, 0, tzinfo=timezone.utc)] * 3,
        "user_id": [200+i, 201+i, 202+i],
        "amount": [1.0*i, 2.0*i, 3.0*i],
        "location": [{"country": "DE", "city": "Frankfurt"}] * 3,
    })
    table.append(data)
    table = catalog.load_table("lab.events")  # refresh

# Now count metadata files in MinIO console.
# How many .metadata.json files?
# How many manifest list (snap-*.avro) files?
# How many manifest (*-m*.avro) files?
```

You should see one new of each per append. **Insight:** every commit produces metadata. After 1000 commits, you have 1000+ snapshots and proportional metadata bloat. This is why `expire_snapshots` exists (Experiment 10).

---

## 📚 Theory deep-dive

### The query planning algorithm

When DuckDB (or Trino, or Spark) executes a query against an Iceberg table:

```
1. Ask catalog: "where is the current metadata for lab.events?"
   → Catalog returns "s3://warehouse/lab/events/metadata/v3.metadata.json"

2. Read metadata.json. Extract:
   - Current schema (to validate the query references valid columns)
   - Current partition spec
   - Current snapshot ID and its manifest-list location

3. Read manifest list. For each manifest entry:
   - Check partition summary against query predicates
   - If predicate doesn't intersect this manifest's partitions, SKIP this manifest entirely
   - Otherwise, add this manifest to the read plan

4. Read each surviving manifest. For each data_file entry:
   - Check column lower_bound/upper_bound against query predicates
   - If predicate cannot match, SKIP this data file
   - Otherwise, add this Parquet file to the read plan

5. Open and read only the surviving Parquet files.
   - Within each Parquet file, use row-group statistics for further pruning.
```

**Three levels of pruning before any data is read.** This is the source of Iceberg's query planning speed.

### Why this is faster than Hive

Hive's metastore stores partition locations. To plan a query on a Hive table, the query engine:

1. Asks the metastore for partition list
2. **Issues `LIST` calls to S3 for each candidate partition** to find files
3. Opens each file

Step 2 is the killer. On a large table, `LIST` calls dominate planning time. Iceberg's manifests **eliminate the listing entirely** — every file path is already in metadata.

This is why moving from Hive to Iceberg can drop query planning from minutes to seconds on large tables.

### The "metadata files grow forever" concern

You may have noticed: every commit writes new metadata. **What stops this from growing forever?**

- `metadata.json` keeps a bounded history (configurable, default 100 versions)
- Old `metadata.json` files are typically cleaned by `expire_snapshots`
- Manifest files can be rewritten/compacted (Experiment 09)
- Snapshots that are no longer reachable from any branch/tag/ref can be expired (Experiment 10)

The cost is real but manageable. We'll quantify it in Experiment 10.

---

## 🇪🇺 Regulatory angle

The auditability story is gold for EU FinTech interviews.

**DORA Article 9** requires ICT systems to have **integrity, authenticity, and traceability of data**. The Iceberg metadata model gives you all three by design:

- **Integrity:** column-level statistics in manifests act as a checksum. If a file is modified, the stats no longer match.
- **Authenticity:** every snapshot has a `summary` field recording the operation (append/overwrite/delete) and source. With proper write tooling, you can prove who changed what.
- **Traceability:** the snapshot list is an append-only audit log. You can reconstruct table state at any past moment.

**MiFID II RTS 25** requires firms to be able to reproduce reports as of any historical date. Iceberg's time travel does this natively — `table.scan(snapshot_id=...)` gives you exact-as-of state.

If asked "how would Iceberg help us meet our DORA obligations?" — this is your answer. Few candidates can speak to both technical and regulatory layers simultaneously. **This is your differentiation.**

---

## ✍️ Re-answer the interview question

> "Walk me through what happens when you query an Iceberg table."

Your answer should now cover:

1. Catalog lookup → metadata.json (one I/O)
2. Read metadata.json → find current snapshot's manifest list
3. Read manifest list → prune by partition summary
4. Read surviving manifests → prune by column min/max statistics
5. Open and read only the surviving Parquet files

Practice saying this out loud in 90 seconds. Time yourself. If you can't, you don't know it yet.

---

## 🎁 LinkedIn post draft

> **"I downloaded every metadata file in an Iceberg table and opened it byte by byte. Here's what I learned about why Iceberg is fast."**
>
> Walk through the four-layer model. End with a diagram (you can recreate the one from this experiment). Tag the Iceberg project.

This kind of post regularly gets 500+ impressions in the data engineering community. More importantly, **anyone who reads it and is hiring will remember your name.**

---

## Next up

→ [Experiment 03: The catalog layer](03-catalog-layer.md) — we'll dig into what a catalog really does, compare SqlCatalog / RestCatalog / GlueCatalog / NessieCatalog, and understand why catalog choice is the most operational-impact decision in an Iceberg deployment.
