# Experiment 14 — Row-Level Mutations: MERGE, UPDATE, DELETE

> **Time:** 120 min · **Tier:** Production · **Prerequisites:** Experiments 01–10 · **Spark profile required**

---

## 🎯 The interview questions this answers

> **"How does Iceberg implement DELETE and UPDATE physically? Walk me through copy-on-write vs merge-on-read. What are position deletes vs equality deletes, and when does each one get written?"**

Pause. Open `interview-faq.md`. Write your current answer in 2–3 sentences. If your answer is "Iceberg overwrites the file," it is wrong on purpose — by the end of this experiment you'll know exactly why.

**Why this matters:** This is the #1 question that separates "I've used Iceberg through SQL" from "I've actually run an Iceberg lakehouse." Every production CDC pipeline, every GDPR erasure workflow, every "fix the bad rows from yesterday" job touches this code path. Get the physical model wrong and your tuning will be wrong.

---

## TL;DR (read this last, not first)

Iceberg V2 has **two ways to delete a row**:

1. **Copy-on-Write (CoW)** — read the file containing the row, write a new file without that row, swap the manifest entry. The old file becomes unreferenced.
2. **Merge-on-Read (MoR)** — write a small **delete file** that says "row at position N in file X is dead" (position delete) or "rows where pk = 42 are dead" (equality delete). The original data file is not rewritten.

CoW makes writes slow and reads fast. MoR makes writes fast and reads slow. **You pick per table, per operation, per use case.** Iceberg lets you mix.

`MERGE INTO` is the SQL surface that uses these primitives. Underneath, every `WHEN MATCHED THEN UPDATE` is "delete the matching row + append the new one." Every `WHEN MATCHED THEN DELETE` is "write a delete." Every `WHEN NOT MATCHED THEN INSERT` is "append."

---

## 🛠️ Hands-on: see CoW and MoR with your own eyes

### Step 0 — Bring up the Spark profile

```bash
# Main lab must already be up: cd lab && ./init.sh
cd lab/spark-profile
./up.sh
```

Verify:

```bash
curl -s http://localhost:8181/health    # Lakekeeper
curl -s http://localhost:8090/v1/info   # Trino
```

Open a Spark SQL shell:

```bash
docker compose exec spark-iceberg spark-sql
```

You should land in a session where `rest_lab` is the default catalog.

### Step 1 — Create a copy-on-write table (the default)

```sql
CREATE NAMESPACE IF NOT EXISTS demo;

DROP TABLE IF EXISTS demo.orders_cow;

CREATE TABLE demo.orders_cow (
    order_id   BIGINT,
    user_id    BIGINT,
    amount     DOUBLE,
    status     STRING,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (status)
TBLPROPERTIES (
    'format-version'         = '2',
    'write.delete.mode'      = 'copy-on-write',
    'write.update.mode'      = 'copy-on-write',
    'write.merge.mode'       = 'copy-on-write'
);

INSERT INTO demo.orders_cow VALUES
    (1, 100, 49.99, 'pending',   TIMESTAMP '2026-05-01 09:00:00'),
    (2, 101, 19.99, 'pending',   TIMESTAMP '2026-05-01 09:01:00'),
    (3, 102, 99.50, 'pending',   TIMESTAMP '2026-05-01 09:02:00'),
    (4, 100, 14.99, 'completed', TIMESTAMP '2026-05-01 09:03:00'),
    (5, 103,  4.99, 'completed', TIMESTAMP '2026-05-01 09:04:00');
```

Look at the files written:

```sql
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM demo.orders_cow.files;
```

You should see **two data files** — one per partition (`status=pending`, `status=completed`). Note their paths.

### Step 2 — Delete one row, in CoW mode

```sql
DELETE FROM demo.orders_cow WHERE order_id = 2;

SELECT file_path, record_count
FROM demo.orders_cow.files;
```

What you should observe:

- `status=pending` partition now has a **different file path** with **2 records** instead of 3.
- `status=completed` partition file is **unchanged** (because the delete didn't touch that partition).
- The old `status=pending` file with 3 records is no longer listed in `.files` (current snapshot), but **it still exists in S3** — visible only to time-travel queries until `expire_snapshots` removes it.

Confirm time travel still sees the old row:

```sql
SELECT order_id FROM demo.orders_cow.history;
-- Note the snapshot_id BEFORE the DELETE. Call it SNAP_BEFORE.

SELECT order_id FROM demo.orders_cow VERSION AS OF <SNAP_BEFORE>;
-- Should still show order_id 2.
```

**The cost of CoW**: any change, even to a single row, rewrites the entire data file containing that row. For a 100 MB file with 1M rows, deleting 1 row = writing 100 MB.

### Step 3 — Create a merge-on-read table

```sql
DROP TABLE IF EXISTS demo.orders_mor;

CREATE TABLE demo.orders_mor (
    order_id   BIGINT,
    user_id    BIGINT,
    amount     DOUBLE,
    status     STRING,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (status)
TBLPROPERTIES (
    'format-version'         = '2',
    'write.delete.mode'      = 'merge-on-read',
    'write.update.mode'      = 'merge-on-read',
    'write.merge.mode'       = 'merge-on-read'
);

INSERT INTO demo.orders_mor SELECT * FROM demo.orders_cow VERSION AS OF <SNAP_BEFORE>;
```

(Use the snapshot_id from before the CoW delete, so this table also starts with 5 rows.)

### Step 4 — Delete one row, in MoR mode

```sql
DELETE FROM demo.orders_mor WHERE order_id = 2;
```

Now inspect the file layout more carefully:

```sql
SELECT content, file_path, record_count, file_size_in_bytes
FROM demo.orders_mor.files;
```

You should see **three rows**:

| content | what it is |
|---|---|
| `0` | a data file (a normal Parquet) |
| `0` | a second data file (the other partition's data) |
| `1` | a **position delete file** |

`content = 1` means "this file lists positions in data files that should be skipped." The original `status=pending` data file is **unchanged** — Iceberg simply added a tiny delete file that says "row at position 1 in that file is dead."

Look at the delete file content:

```sql
SELECT *
FROM demo.orders_mor.entries
WHERE data_file.content = 1;
```

You'll see the delete file's path, its `record_count` (= 1), and which data file it applies to (via `data_file.referenced_data_file`).

**The win of MoR**: deleting 1 row wrote a ~1 KB delete file, not a 100 MB rewrite.

### Step 5 — Pay the read cost of MoR

Now query the table:

```sql
SELECT * FROM demo.orders_mor;
```

Behind the scenes, the reader:

1. Lists data files from the manifest.
2. Lists delete files from the manifest.
3. For each data file, applies any matching delete file: skip the listed positions, return the rest.

This is the **merge** in merge-on-read. Every read pays this cost as long as delete files exist. The more deletes accumulate, the slower reads get. This is why MoR tables need **periodic compaction** to materialize the deletes back into data files.

Run compaction:

```sql
CALL rest_lab.system.rewrite_data_files(
    table => 'demo.orders_mor',
    options => map('delete-file-threshold', '1')
);
```

Then re-inspect:

```sql
SELECT content, COUNT(*) FROM demo.orders_mor.files GROUP BY content;
```

Delete files should be gone. The data files have been rewritten with the deletes materialized in.

### Step 6 — MERGE INTO (CDC pattern)

This is the bread-and-butter of production Iceberg use. Simulate a CDC source with both updates and inserts:

```sql
CREATE OR REPLACE TEMP VIEW cdc_batch AS
SELECT * FROM VALUES
    (1, 100, 49.99, 'completed', TIMESTAMP '2026-05-01 10:00:00'),  -- status changed
    (3, 102, 99.50, 'cancelled', TIMESTAMP '2026-05-01 10:01:00'),  -- status changed
    (6, 104, 29.00, 'pending',   TIMESTAMP '2026-05-01 10:02:00'),  -- new row
    (7, 105,  9.99, 'pending',   TIMESTAMP '2026-05-01 10:03:00')   -- new row
AS t(order_id, user_id, amount, status, updated_at);

MERGE INTO demo.orders_mor t
USING cdc_batch s
ON t.order_id = s.order_id
WHEN MATCHED AND s.status = 'cancelled' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM demo.orders_mor ORDER BY order_id;
```

You should see:

- `order_id = 1` → status changed from `pending` to `completed`
- `order_id = 3` → gone (DELETE branch)
- `order_id = 6, 7` → inserted

Look at what files this produced:

```sql
SELECT content, file_path, record_count
FROM demo.orders_mor.files
WHERE file_path LIKE '%'
ORDER BY content;
```

In MoR mode, the UPDATE was implemented as **(equality delete on order_id) + (append new row)**. Spark may write an **equality delete file** (`content = 2`) rather than position delete, because at MERGE plan time it doesn't know which file position holds each matching row.

Inspect:

```sql
SELECT data_file.content, data_file.equality_ids, data_file.record_count, data_file.file_path
FROM demo.orders_mor.entries
ORDER BY data_file.content DESC;
```

- `content = 0` → data file
- `content = 1` → position delete (file path + position offsets)
- `content = 2` → equality delete (column values that match are dead)

### Step 7 — Compare write/read cost CoW vs MoR

Run the same MERGE against `demo.orders_cow`:

```sql
MERGE INTO demo.orders_cow t
USING cdc_batch s
ON t.order_id = s.order_id
WHEN MATCHED AND s.status = 'cancelled' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Now look at how each table evolved:

```sql
SELECT 'cow' AS mode,
       COUNT(*) FILTER (WHERE content = 0) AS data_files,
       COUNT(*) FILTER (WHERE content = 1) AS pos_deletes,
       COUNT(*) FILTER (WHERE content = 2) AS eq_deletes,
       SUM(file_size_in_bytes)            AS total_bytes
FROM demo.orders_cow.files
UNION ALL
SELECT 'mor', COUNT(*) FILTER (WHERE content = 0), COUNT(*) FILTER (WHERE content = 1), COUNT(*) FILTER (WHERE content = 2), SUM(file_size_in_bytes)
FROM demo.orders_mor.files;
```

CoW: more data file bytes (rewrote partitions), zero delete files. MoR: less data file bytes (kept originals), but multiple small delete files.

**The tradeoff in one line:** CoW pays at write time, once. MoR pays at read time, every read, until compaction.

---

## 💥 Break it

### Break 1: kill a delete file in MinIO

In the MinIO console (`http://localhost:9001`), navigate to the `orders_mor` table prefix, find a file matching `*-deletes-*` and delete it.

Back in Spark:

```sql
SELECT * FROM demo.orders_mor;
```

What do you see? The previously-deleted row reappears. **Insight:** delete files are load-bearing. They are not a "convenience cache." Lose one and you've changed the answer.

Reset:

```sql
CALL rest_lab.system.rollback_to_snapshot('demo.orders_mor', <PREVIOUS_SNAP>);
```

(Or run the main lab's `./reset.sh --confirm` to wipe and restart.)

### Break 2: read MoR from an engine that doesn't support V2

Try the same MoR table from DuckDB in the main lab Jupyter:

```python
import duckdb
from src.catalog_helper import configure_duckdb_for_minio

con = duckdb.connect()
configure_duckdb_for_minio(con)
con.execute("SELECT * FROM iceberg_scan('s3://warehouse/demo/orders_mor/')")
```

Depending on DuckDB's iceberg extension version, you may get either:

- A correct result (modern DuckDB has V2 delete file support), or
- A wrong result that ignores the deletes, or
- An error.

**Insight:** "Iceberg is open" does not mean every engine reads every feature. **Equality deletes** in particular were the last MoR feature to land in non-Spark engines. Production rule: if your reader fleet includes mixed engines, **stay in CoW** unless you've explicitly tested MoR with every reader.

### Break 3: let MoR delete files pile up

In a notebook:

```python
from pyspark.sql import SparkSession  # if you mount this; otherwise drive from spark-sql

# Pseudocode — in spark-sql:
# Run 200 single-row deletes against demo.orders_mor.
# Then SELECT * FROM demo.orders_mor and time it.
```

```sql
-- In spark-sql:
INSERT INTO demo.orders_mor SELECT order_id + 1000, user_id, amount, status, updated_at FROM demo.orders_mor;

-- Generate many deletes
DELETE FROM demo.orders_mor WHERE order_id = 1001;
DELETE FROM demo.orders_mor WHERE order_id = 1002;
DELETE FROM demo.orders_mor WHERE order_id = 1003;
-- ... repeat in a loop

SELECT COUNT(*) AS data_files,
       (SELECT COUNT(*) FROM demo.orders_mor.files WHERE content > 0) AS delete_files
FROM demo.orders_mor.files WHERE content = 0;
```

Watch delete file count climb. Time a full scan before and after running `rewrite_data_files`. **Insight:** without scheduled compaction, MoR tables degrade silently. The query plan still works, the answer is correct, but the latency creeps up. By the time a user files a ticket, you have 10,000 delete files.

### Break 4: MERGE with a non-unique join key

```sql
CREATE TEMP VIEW bad_source AS SELECT 1 AS order_id, 100 AS user_id, 49.99 AS amount, 'completed' AS status, TIMESTAMP '2026-05-01' AS updated_at
UNION ALL
SELECT 1, 100, 49.99, 'cancelled', TIMESTAMP '2026-05-01';

MERGE INTO demo.orders_cow t USING bad_source s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *;
```

You should get an error along the lines of "the ON search condition of the MERGE statement matched a single row from the target table with multiple rows of the source table." **Insight:** Iceberg (via Spark) enforces uniqueness in MERGE. CDC pipelines that emit duplicate keys for the same target row will fail loudly — this is a feature, not a bug, because the alternative is silent data corruption.

---

## 📚 Theory deep-dive

### The V2 spec, in one paragraph

Iceberg V1 was append-only at the file level — to delete or update, you had to rewrite the whole data file (CoW only). V2 added two new entries to the manifest: **position delete files** and **equality delete files**. This is the entire mechanism behind merge-on-read. The spec lives at https://iceberg.apache.org/spec/.

### Position deletes vs equality deletes

| | Position delete | Equality delete |
|---|---|---|
| What it stores | `(referenced_data_file, position_in_file)` | `(column_values...)` matching dead rows |
| When written | When the writer knows the exact file+position of dead rows (e.g., a Spark CoW-style plan that decided to switch to MoR for cost reasons; or a small batch DELETE WHERE) | When the writer knows the predicate but not the physical row location yet (most MERGE statements) |
| Read cost | Cheap — bitmap lookup per data file | Expensive — left-anti-join on the equality columns per data file |
| Compaction priority | Lower (cheap to read) | Higher (expensive to read) |

In production: **most MoR deletes are equality deletes**, because MERGE plans don't pre-locate rows. This is why MoR tables get slow fast.

### When CoW, when MoR

| Workload | Recommendation |
|---|---|
| Append-mostly (events, logs) with rare deletes | CoW. Deletes are rare; rewriting is cheap because data files are big and aligned with partitions. |
| CDC sink with high-frequency upserts | MoR. CoW would constantly rewrite hot partitions. |
| GDPR erasure workflow (low frequency, exact rows) | CoW. Erasure is rare; you want a "clean" data file so audit doesn't need to chase delete files. |
| Streaming staging → batch silver | Mixed: MoR on bronze, CoW on silver after a compaction step. |
| Heterogeneous reader fleet (Spark + Trino + Snowflake + Athena + custom service) | Default CoW until you've tested MoR end-to-end on every reader. |

### The three table properties to know

| Property | Default in V2 | Notes |
|---|---|---|
| `write.delete.mode` | `copy-on-write` (Spark default) | controls `DELETE FROM` |
| `write.update.mode` | `copy-on-write` | controls `UPDATE SET` |
| `write.merge.mode` | `copy-on-write` | controls `MERGE INTO` |

You can mix: e.g., `write.delete.mode=copy-on-write` (rare clean deletes) + `write.merge.mode=merge-on-read` (frequent CDC merges).

### Compaction policy for MoR

Rule of thumb: **delete files should never exceed ~5% of data file bytes** in a partition. Past that, read amplification dominates.

Two compaction primitives:

```sql
-- Materialize position deletes into data files (cheap)
CALL rest_lab.system.rewrite_position_delete_files(table => 'demo.orders_mor');

-- Full rewrite that also handles equality deletes (expensive)
CALL rest_lab.system.rewrite_data_files(
    table => 'demo.orders_mor',
    options => map('delete-file-threshold', '2', 'min-input-files', '5')
);
```

Schedule the cheap one frequently (hourly), the expensive one rarely (daily / per partition watermark).

### The MERGE planner: why duplicates fail

`MERGE INTO target USING source ON cond` requires that no target row is matched by more than one source row. Otherwise the update is non-deterministic ("which source row wins?"). Spark enforces this with a join-cardinality check. The implication for CDC: your source must be deduplicated *before* MERGE. Common patterns:

- Window over `updated_at DESC` + `row_number = 1` per primary key
- Read changelog watermark + dedupe in the same stage as the MERGE source view

### Mental model for what each statement produces

In V2 MoR:

| Statement | New data files | Position deletes | Equality deletes |
|---|---|---|---|
| `INSERT` | yes | no | no |
| `DELETE WHERE pk = c` | no | maybe (depends on planner) | yes (most common) |
| `UPDATE WHERE pk = c SET …` | yes (the new rows) | no | yes (kill the old rows) |
| `MERGE WHEN MATCHED THEN UPDATE` | yes | no | yes |
| `MERGE WHEN MATCHED THEN DELETE` | no | no | yes |
| `MERGE WHEN NOT MATCHED THEN INSERT` | yes | no | no |

In V2 CoW: data files get rewritten end-to-end; no delete files are ever produced.

### Production gotcha: delete file format

Position delete files are Parquet by default. Equality delete files are Parquet by default. Some older engine versions wrote Avro delete files. Mixing formats across engines used to cause incompatibility — modern engines should all read both, but the spec allows either. If you write deletes from Engine A and read from Engine B, **test it**.

---

## ✍️ Re-answer the interview question

> "How does Iceberg implement DELETE / UPDATE? CoW vs MoR? Position vs equality deletes?"

Cover, in this order:

1. V2 spec added two delete-file types. V1 was CoW-only.
2. **CoW**: rewrite the data file without the dead rows. Cheap reads, expensive writes.
3. **MoR**: write a tiny delete file. Cheap writes, expensive reads (until compaction).
4. **Position deletes** target `(file, position)` — cheap to apply. **Equality deletes** target column values — expensive to apply (planner doesn't know positions yet).
5. `MERGE INTO` is the compound: matched-delete + matched-update + not-matched-insert, each of which uses the above primitives.
6. Engine compatibility: equality deletes are the slowest-to-land feature across the ecosystem; on a mixed-reader fleet, default to CoW.
7. Operational implication: MoR needs scheduled compaction; CoW does not (but has 100x write amplification on single-row deletes).

---

## 🎁 LinkedIn post draft

> **"Iceberg has *two* ways to delete a row, and most teams pick the wrong one. Here's how to know which is right for your table."**
>
> Walk through CoW vs MoR, position vs equality deletes, and the compaction-policy implications. End with: "Choose per-table, not per-codebase. Append-mostly bronze tables are MoR. GDPR-erasure silver tables are CoW. There is no universally right answer — only the one that matches your write pattern."

This post performs exceptionally well — senior data engineers and platform leads see it as honest signal.

---

## Session wrap-up (close the loop)

1. Confirm hands-on: both `demo.orders_cow` and `demo.orders_mor` exist, you've inspected `.files` for both, seen delete files in MoR, run a MERGE, and compacted MoR back to a single data file per partition.
2. Update `interview-faq.md` with your **after** answer.
3. If Break it left things broken: `CALL rest_lab.system.rollback_to_snapshot(...)` or run the main lab's `./reset.sh --confirm` to wipe.
4. End of day: `cd lab/spark-profile && ./down.sh` (keeps state), then `cd lab && docker compose stop`.
5. Full cleanup: `./down.sh` here + `rm -rf state/`, then main lab §10.

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md).

---

## Next up

→ [Experiment 15: CDC & incremental reads](15-cdc-incremental-reads.md) — now that you can write upserts, you'll learn how downstream consumers read **only what changed** since their last run, without re-scanning the whole table.
