# Experiment 21 — Hive → Iceberg Migration

> **Time:** 120 min · **Tier:** Production · **Prerequisites:** Experiments 01–10, 16 (REST catalog), 20 (multi-engine) · **Spark profile required**

---

## 🎯 The interview question this answers

> **"How do you migrate a 50 TB Hive table to Iceberg with zero downtime? What's the difference between `add_files`, `snapshot`, and full rewrite? What validation do you run after cutover?"**

Pause. Open `interview-faq.md`. Write your current answer.

**Why this matters:** This is the **#1 reason** companies adopt Iceberg in 2026 — they have a legacy Hive lakehouse and want the ACID/schema/compaction wins without rewriting petabytes. Walking through this migration in an interview signals "I've actually done this in production." Most candidates have only read about it.

---

## TL;DR

Three strategies, in increasing cost and decreasing risk:

1. **`add_files`** (in-place) — point Iceberg at the *existing* Parquet files. Zero data movement. Catch: Hive partition layout must match Iceberg's hidden-partitioning rules, or you need a partition spec that's literal-identity.
2. **`snapshot`** action (shadow migration) — Iceberg creates a *new* table that *references the same Parquet files* via Iceberg metadata. Original Hive table still works. You can write to the Iceberg shadow without affecting Hive readers. Cutover is a config flip.
3. **Full rewrite** — read from Hive, write fresh into Iceberg with the schema and partitioning you want. Most expensive (2x storage during, full read+write IO), highest safety, gives you a clean slate.

In production: `snapshot` is the standard playbook. It's cheap, reversible, and gives you a parallel-write period where you can validate before flipping reads.

---

## 🛠️ Hands-on: simulate a Hive table, then migrate it three ways

We don't have Hive Metastore in the lab, but we have what matters: **Parquet files in a Hive-style directory layout** registered as an Iceberg "external" table. This simulates the migration source exactly.

### Step 0 — Spark profile up

```bash
cd lab && ./init.sh
cd lab/spark-profile && ./up.sh
docker compose exec spark-iceberg spark-sql
```

### Step 1 — Build a "Hive-style" source dataset

Hive stores partitioned tables as directory hierarchies: `bucket/db/table/dt=2026-05-15/country=DE/part-00000.parquet`. We'll create that exact layout in MinIO and pretend it's Hive output.

```sql
-- A staging Iceberg table just to *generate* the Parquet files. We'll re-read them as raw later.
CREATE NAMESPACE IF NOT EXISTS hivelike;

DROP TABLE IF EXISTS hivelike.gen;
CREATE TABLE hivelike.gen (
    event_id BIGINT,
    user_id BIGINT,
    amount DOUBLE,
    dt STRING,
    country STRING
)
USING iceberg
PARTITIONED BY (dt, country);

INSERT INTO hivelike.gen VALUES
    (1, 100, 9.99,  '2026-05-15', 'DE'),
    (2, 101, 19.99, '2026-05-15', 'DE'),
    (3, 102, 29.99, '2026-05-15', 'FR'),
    (4, 103, 39.99, '2026-05-16', 'DE'),
    (5, 104, 49.99, '2026-05-16', 'NL'),
    (6, 105, 59.99, '2026-05-16', 'NL'),
    (7, 106, 69.99, '2026-05-17', 'DE'),
    (8, 107, 79.99, '2026-05-17', 'FR'),
    (9, 108, 89.99, '2026-05-17', 'FR');

-- Find where Iceberg put the data
SELECT DISTINCT regexp_extract(file_path, '(s3://[^/]+/[^/]+/[^/]+/[^/]+)/', 1)
FROM hivelike.gen.files;
```

In a real migration this is the source — a Hive table whose Parquet files exist at a known S3 path with `partition_col=value/` directories.

### Step 2 — Strategy 1: `add_files` (in-place migration)

The cheapest strategy. We declare an empty Iceberg table with the *same* schema and partition spec, then point it at the existing Parquet files.

```sql
DROP TABLE IF EXISTS hivelike.migrated_inplace;

CREATE TABLE hivelike.migrated_inplace (
    event_id BIGINT,
    user_id BIGINT,
    amount DOUBLE,
    dt STRING,
    country STRING
)
USING iceberg
PARTITIONED BY (dt, country);

-- Now add the existing Parquet files. The procedure scans the source partitions,
-- registers each file as an Iceberg data file, and computes column-level stats.
CALL rest_lab.system.add_files(
    table => 'hivelike.migrated_inplace',
    source_table => 'hivelike.gen'  -- in a real Hive migration: '`spark_catalog`.default.hive_source_table'
);

SELECT count(*) FROM hivelike.migrated_inplace;
SELECT content, file_path, record_count FROM hivelike.migrated_inplace.files ORDER BY file_path;
```

What just happened, physically:

- **Zero Parquet files moved.** Same S3 paths.
- Iceberg wrote: a metadata.json, a manifest list, manifests, all pointing at the existing Parquet paths.
- The original Hive table is **still pointing at the same files**. Both tables can be queried.

This is the literal in-place migration. Minutes for 50TB. The catch:

- The source Parquet schemas must be consistent (no per-file schema drift, which is common in old Hive tables).
- Column-level statistics are computed by reading Parquet footers — Iceberg won't open every Parquet file, but it does read the footers, which is fast.
- The Hive partition columns must match Iceberg's partition spec exactly. If Hive partitioned by `dt` and you want Iceberg to partition by `days(event_ts)` (hidden partitioning), `add_files` won't help — the partition values aren't computable from the existing data.

### Step 3 — Read the migrated table from Trino

```sql
-- Switch terminals:
-- docker compose exec trino trino
SELECT * FROM iceberg.hivelike.migrated_inplace ORDER BY event_id;
```

Same data, accessible from any Iceberg-aware engine. **You just did a "Hive → Iceberg" migration without moving a single byte of data.**

### Step 4 — Strategy 2: `snapshot` action (shadow migration)

`snapshot` is `add_files` plus a copy-on-write safety net: Iceberg creates a new table that references the existing files **read-only**, while leaving the original Hive table writable. This is the production cutover pattern.

```sql
-- In Spark
DROP TABLE IF EXISTS hivelike.migrated_shadow;

CALL rest_lab.system.snapshot(
    source_table => 'hivelike.gen',
    table        => 'hivelike.migrated_shadow'
);

SELECT count(*) FROM hivelike.migrated_shadow;
SELECT * FROM hivelike.migrated_shadow.files;
```

The shadow table behaves like an Iceberg table for reads. Writes to it are routed through Iceberg's write path. Writes to the source (Hive) keep going to Hive. **The two tables drift apart from this moment on** — that's intentional. The migration playbook is:

1. Run `snapshot` → both tables exist, identical.
2. Dual-write: any new data goes to both Hive and Iceberg in your ETL pipeline.
3. Validate: query both, compare results.
4. Cut over readers: point production reads at Iceberg.
5. Decommission: stop writing to Hive.

Until step 5, you can roll back at any moment by repointing readers at Hive.

### Step 5 — Strategy 3: full rewrite

Sometimes you *want* to rewrite. Reasons:

- Hive table has schema drift (different Parquet files have different schemas — totally legal in Hive, illegal in Iceberg).
- You want a different partition spec (e.g., move from `dt STRING` to `days(event_ts)` hidden partitioning).
- You want to apply a sort order during the migration.
- You want a clean lineage start.

```sql
DROP TABLE IF EXISTS hivelike.migrated_rewrite;

-- Create a fresh table with the schema/partitioning you actually want
CREATE TABLE hivelike.migrated_rewrite (
    event_id BIGINT,
    user_id BIGINT,
    amount DOUBLE,
    dt DATE,
    country STRING,
    event_ts TIMESTAMP  -- new column, derived from dt
)
USING iceberg
PARTITIONED BY (days(event_ts), country)
TBLPROPERTIES ('format-version' = '2');

-- Migrate with transformation
INSERT INTO hivelike.migrated_rewrite
SELECT
    event_id,
    user_id,
    amount,
    cast(dt AS DATE) AS dt,
    country,
    cast(concat(dt, ' 00:00:00') AS TIMESTAMP) AS event_ts
FROM hivelike.gen;

SELECT * FROM hivelike.migrated_rewrite ORDER BY event_id;
```

Cost: read all source data, write all of it again. 2x IO during migration; ~1x extra storage until the source is decommissioned. For 50TB this can be days of pipeline time and serious cost.

**Decision matrix:**

| If… | Use |
|---|---|
| Source schema is clean, partitioning is OK, you want speed | `add_files` |
| You want a safety net + dual-write period | `snapshot` |
| You're changing schema, partitioning, or want hidden partitioning | full rewrite |

### Step 6 — Validation: prove the migration didn't lose anything

Whatever strategy you used, **run the same queries on source and target and compare**. The shape of this for a real migration:

```sql
-- Row counts must match
SELECT 'source' AS t, count(*) FROM hivelike.gen
UNION ALL
SELECT 'inplace', count(*) FROM hivelike.migrated_inplace
UNION ALL
SELECT 'shadow', count(*) FROM hivelike.migrated_shadow
UNION ALL
SELECT 'rewrite', count(*) FROM hivelike.migrated_rewrite;

-- Aggregations must match
SELECT 'source' AS t, country, sum(amount) FROM hivelike.gen GROUP BY country
UNION ALL
SELECT 'inplace', country, sum(amount) FROM hivelike.migrated_inplace GROUP BY country
UNION ALL
SELECT 'rewrite', country, sum(amount) FROM hivelike.migrated_rewrite GROUP BY country
ORDER BY t, country;

-- Row-level checksums (the gold standard)
SELECT 'source' AS t,
       md5(cast(collect_list(struct(event_id, user_id, amount, dt, country)) AS string)) AS checksum
FROM (SELECT * FROM hivelike.gen ORDER BY event_id) s;

-- Run the same checksum against each migrated table.
-- If checksums match → byte-for-byte equivalent.
-- If aggregations match but checksums don't → ordering or null-handling drift.
-- If aggregations don't match → real data loss; do not cut over.
```

In production: a validation job runs nightly during the dual-write period. If checksums drift for three consecutive days, the cutover is rolled back.

### Step 7 — Cutover playbook

The "zero-downtime" claim comes from this sequence:

```
Day -7:   snapshot the Hive table → Iceberg shadow exists
Day -7 onwards:  every ETL job writes to BOTH Hive (existing) and Iceberg (shadow)
Day -7 onwards:  validation job runs nightly, compares checksums
Day  0:   stop writing to Hive in the ETL job (Iceberg-only writes)
Day  0:   flip a config flag in the read-path service; reads go to Iceberg
Day +1:   spot-check production metrics, alerts, dashboards
Day +7:   if everything's stable, mark Hive table as decommissioned
Day +30:  delete the Hive metastore registration (NOT the Parquet files — those are still referenced by Iceberg if you used add_files / snapshot)
```

The "zero downtime" part is that the read flip is a single config change, and the dual-write period means you have a tested rollback path.

### Step 8 — Rollback drill

What if cutover went bad and you need to revert? Two scenarios:

**Scenario A: `snapshot` or `add_files` migration, no destructive changes.**
- Just point readers back at the Hive table. Hive still has its registration, still has its data.
- The Iceberg shadow becomes inert but not a problem.

**Scenario B: full rewrite, Hive table since dropped.**
- Hive metastore registration is gone, but **the Parquet files are still in S3** (you didn't `PURGE` them, right?).
- Re-register them in HMS, point readers back.
- This works only if you preserved the original files. In a full rewrite migration, **never `DROP TABLE … PURGE`** on the source until you've been on Iceberg for 30+ days.

### Step 9 — The "consolidate small files post-migration" step

A Hive table that's been around for years usually has the small-files problem (every nightly ETL added a small file). After `add_files` / `snapshot`, the Iceberg table inherits that file count. **Run a compaction pass post-migration:**

```sql
CALL rest_lab.system.rewrite_data_files(
    table => 'hivelike.migrated_shadow',
    options => map('target-file-size-bytes', '536870912')  -- 512 MB
);
```

This is the moment to also apply a sort order if you've decided one (experiment 19). The new files will be sorted; old files will be rewritten and dropped.

---

## 💥 Break it

### Break 1: schema drift in the source

Real Hive tables often have schema drift — column added in 2019, column renamed in 2022. The Parquet files don't all share one schema. Try this:

```python
# Conceptually: write two Parquet files with different schemas under the same partition
# (column `extra` exists in one, missing in the other)
```

```sql
CALL rest_lab.system.add_files(table => 'hivelike.migrated_drift', source_table => 'hivelike.drift_source');
```

`add_files` may succeed but then queries return errors when reading the file with the missing column. **Production rule:** before `add_files`, validate the source has consistent schema across all partitions. The `parquet-tools schema` command on a sample is the standard check.

### Break 2: partition column type mismatch

Hive often stores `dt STRING = '2026-05-15'`. Iceberg with hidden partitioning would want `event_ts TIMESTAMP` with `days()` transform. These two are *not equivalent* — the string-typed Hive partition can have arbitrary values (`'2026-05-15'`, `'2026/05/15'`, `'May 15 2026'` — all legal strings, all distinct partitions). The Iceberg `days()` transform yields deterministic dates.

`add_files` requires the partition columns to be representable in the new spec. If they're not (string dates that aren't always-parseable), you need a full rewrite or a schema-cleanup pass before migration.

### Break 3: drop Hive too soon

Day 1 after cutover, you `DROP TABLE hive_db.events PURGE` to "free up space." Day 3, your finance team finds a discrepancy and you want to roll back. **The Parquet files are gone.** If you used `add_files` or `snapshot`, the Iceberg table's data files are *those same files* — also gone. The Iceberg table now points at dead paths.

This is the worst-case migration failure. **Never** `PURGE` the source until you've been live for at least 30 days *and* you've validated that the Iceberg table's files survive a `remove_orphan_files` dry run.

### Break 4: dual-write divergence

During the dual-write period, your ETL pipeline writes to both tables. But: Hive's write isn't atomic, Iceberg's is. A pipeline crash that successfully wrote to Hive but failed to commit to Iceberg → divergence. Or vice versa.

Production fix: dual-write should be either:
- **Synchronous, both-or-nothing**: write to Iceberg first (atomic), then Hive. On Iceberg-success-Hive-failure, you have Hive lag → catch up in the next batch.
- **Asynchronous, validate-and-converge**: write to Iceberg only, replicate from Iceberg to Hive on a delay using Iceberg's incremental scan (experiment 15). Hive lags behind by ~minutes.

The second option is more elegant if you can afford the Hive read-side lag.

### Break 5: forgot to compact post-migration

Six months after migration, the Iceberg table is "slow." Investigation: it has 200,000 tiny files (inherited from Hive's 6 years of nightly appends). Compaction was never run because nobody owned it. **Insight:** the cutover playbook must include the post-migration compaction step. Otherwise you've migrated the *data* but not the *operational characteristics* — and Iceberg's value mostly lives in those characteristics.

---

## 📚 Theory deep-dive

### Why these three strategies, and not others

Iceberg metadata is a tree of pointers. The data files at the leaves can be:
- Pre-existing Parquet you didn't write (`add_files`, `snapshot`)
- Brand new Parquet you wrote (`INSERT … SELECT` from the source)

That binary choice gives you the three strategies. There's no fourth.

### `add_files` vs `snapshot` semantically

| | `add_files` | `snapshot` |
|---|---|---|
| Creates a new table | yes (you create it first, empty) | yes (procedure creates it) |
| Source table still readable | yes | yes |
| Source table still writable | yes (writes don't appear in target) | yes (writes don't appear in target) |
| Re-runnable to pick up new files | yes (idempotent on already-added files) | usually re-run as `add_files` after initial snapshot |
| Common in production | yes | yes (more common for cutover) |

In practice many teams use `snapshot` once to bootstrap, then `add_files` repeatedly to add new partitions written by Hive during the dual-write period.

### What `add_files` actually does internally

For each Parquet file in the source:
1. Open the Parquet footer (no full read).
2. Extract per-column min/max stats.
3. Extract row count, file size.
4. Compute the partition tuple from the directory structure (Hive layout) or column values.
5. Append to a manifest as a `data_file` entry.

For 50 TB / 100,000 files this is on the order of **tens of minutes**, not days. The cost is footer reads, not data reads.

### Migration risk hierarchy

From safest to riskiest:

1. **Read-side migration** (a new Iceberg shadow, all writes still go to Hive, reads start moving over) — fully reversible.
2. **Dual-write migration** (writes go to both, reads gradually move) — reversible until you stop writing to Hive.
3. **Cutover with retention** (writes to Iceberg only, Hive kept around 30+ days) — recoverable but takes a few hours.
4. **Cutover with immediate decommission** (Hive dropped) — only safe if you've held the previous step for weeks.

Most production migrations live in step 2 → 3 for 30 days, then 3 → 4 with `add_files`-style retention (keep the Hive registration without writing to it, drop it after a few months).

### Why not just write a Spark job?

"Just read from Hive and write to Iceberg" is the naive answer, and it's the right one for tables under ~1 TB. Above that, the time/cost arithmetic flips:

- 50 TB read at 1 GB/s = ~14 hours, just for the read.
- Plus the write (another 14 hours).
- Plus 50 TB of duplicate storage for the migration period.

`add_files` is **minutes**. The only reason to choose a full rewrite is if you need the schema/partition cleanup it gives.

### Hive Metastore → REST catalog: a separate concern

Migrating *data* from Hive-laid-out Parquet to Iceberg metadata is what we've covered. Migrating the *catalog* (HMS → REST/Lakekeeper/Polaris) is independent. Most teams do both, often sequentially:

- Phase 1: migrate tables to Iceberg metadata, leave them registered in HMS via HiveCatalog.
- Phase 2: migrate catalog from HiveCatalog to REST catalog (experiment 16). Tables don't move, just the registration.

Doing both at once is possible but harder to roll back.

### When migration fails

Real failure modes seen in production:

- **Source has bad Parquet files** (corrupted footers). `add_files` fails on those. Fix: scan + quarantine before migration.
- **Source has subtly inconsistent partition columns** (e.g., trailing-whitespace string partitions). Iceberg's strictness exposes this. Fix: rewrite affected partitions.
- **Source partition values aren't recoverable from data** (Hive let you put anything in a partition column). Fix: full rewrite, or `add_files` with explicit partition_filter.
- **Sheer cost of footer scans** on a 10M-file Hive table → footer scans take days. Fix: parallelize via Spark, accept the cost, OR rewrite incrementally per partition.
- **Validation lies** (your checksum implementation has a bug). Fix: validate with three different validators (count, sum-aggregation, row-checksum) — if all three agree, you're probably fine.

---

## ✍️ Re-answer the interview question

> "How do you migrate a 50 TB Hive table to Iceberg with zero downtime?"

Cover:

1. **Three strategies**: `add_files` (cheap, in-place), `snapshot` (shadow, reversible), full rewrite (expensive, gives schema cleanup).
2. **Default playbook**: `snapshot` + dual-write + checksum validation + read-flip + 30-day retention.
3. **What `add_files` actually does**: reads Parquet footers for stats, no data movement.
4. **The risk hierarchy**: read-side migration → dual-write → cutover-with-retention → decommission.
5. **Post-migration compaction** is part of the playbook, not an afterthought.
6. **Don't `PURGE`** until 30+ days post-cutover.
7. **Catalog migration is separate** — HMS → REST is its own project.

---

## 🎁 LinkedIn post draft

> **"I migrated 50 TB from Hive to Iceberg in minutes, not days. The `add_files` procedure is one of the most under-talked-about wins in modern lakehouse tooling. Here's the playbook that survived contact with production."**
>
> Walk the three strategies + the cutover sequence. End with the post-migration compaction step ("you've migrated the data; now migrate the operations").

This is hiring-magnet content for any platform team running a Hive → Iceberg migration program.

---

## Session wrap-up

1. Confirm hands-on: you have three migrated tables (`migrated_inplace`, `migrated_shadow`, `migrated_rewrite`) all containing equivalent data; you've validated with row counts and aggregations; you've considered the cutover and rollback paths.
2. Update `interview-faq.md`.
3. Spark profile down; main lab down.

---

## Tier 3 (Production) complete ✅

You now have hands-on experience with:

- Small files & compaction (exp 09)
- Snapshot expiration & GC (exp 10)
- Format selection (exp 11)
- Object storage choice (exp 13)
- Row-level mutations: MERGE, CoW vs MoR (exp 14)
- CDC consumers & incremental reads (exp 15)
- Production REST catalog (exp 16)
- Write-Audit-Publish (exp 17)
- Maintenance & observability (exp 18)
- Performance tuning (exp 19)
- Multi-engine interop (exp 20)
- Hive → Iceberg migration (exp 21)

This is the operational surface of a real Iceberg lakehouse. Before moving to Tier 4:

- Re-answer the production questions (16–41) in `interview-faq.md` without notes
- If any answer is fuzzy, redo that experiment — these are the questions production teams will actually ask

---

## Next up

→ [Experiment 12: Iceberg for regulated data](12-iceberg-for-regulated-data.md) — the specialization tier. The same lakehouse you can now operate, defended as a compliance architecture choice for EU regulated environments.
