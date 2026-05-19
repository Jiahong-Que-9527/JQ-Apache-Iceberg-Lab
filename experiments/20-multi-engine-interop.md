# Experiment 20 — Multi-Engine Interop

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Experiments 01–10, 14 (CoW/MoR), 16 (REST catalog) · **Spark profile required**

---

## 🎯 The interview question this answers

> **"Spark writes V2 delete files. Trino reads. Can my Snowflake external table read it? My Athena? My DuckDB? What feature surface should I assume for multi-engine deployments?"**

Pause. Open `interview-faq.md`. Write your current answer.

**Why this matters:** "Iceberg is open" is a marketing line. The reality is a **compatibility matrix** that changes every six months. Every engine implements a different subset of the spec. Knowing which engines support which features — and how to test it for your engine fleet — separates the people who got burned by it once and learned, from the ones who haven't yet.

---

## TL;DR

Three layers of compatibility:

1. **Spec version**: V1 (legacy, no delete files) vs V2 (delete files supported).
2. **V2 read features**: position deletes, equality deletes, sort orders, bloom filters — each landed in different engines in different releases.
3. **V2 write features**: writing V2 delete files (only Spark, Flink, and a few proprietary engines today).

Reader engines: Spark, Trino, DuckDB, PyIceberg, ClickHouse, Snowflake, BigQuery, Athena, Dremio. **All read V1**. Most read V2 data files. **Few read V2 equality deletes correctly.** Production rule: if your fleet includes any engine you haven't tested with V2 delete files, default to **copy-on-write** on shared tables.

---

## 🛠️ Hands-on: write from Spark, read from three engines

### Step 0 — Spark profile up

```bash
cd lab && ./init.sh
cd lab/spark-profile && ./up.sh
```

We'll write tables from Spark and read them from Trino + PyIceberg + DuckDB. All four point at the same Lakekeeper REST catalog and same MinIO bucket.

### Step 1 — From Spark: create a CoW table and a MoR table

```bash
docker compose exec spark-iceberg spark-sql
```

```sql
CREATE NAMESPACE IF NOT EXISTS interop;

DROP TABLE IF EXISTS interop.shared_cow;
CREATE TABLE interop.shared_cow (
    id BIGINT,
    region STRING,
    value DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
    'format-version'    = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write'
);

INSERT INTO interop.shared_cow VALUES
    (1, 'EU', 10.0, TIMESTAMP '2026-05-18 09:00:00'),
    (2, 'EU', 20.0, TIMESTAMP '2026-05-18 09:01:00'),
    (3, 'US', 30.0, TIMESTAMP '2026-05-18 09:02:00'),
    (4, 'US', 40.0, TIMESTAMP '2026-05-18 09:03:00');

DELETE FROM interop.shared_cow WHERE id = 2;
UPDATE interop.shared_cow SET value = 99.0 WHERE id = 3;

DROP TABLE IF EXISTS interop.shared_mor;
CREATE TABLE interop.shared_mor (
    id BIGINT,
    region STRING,
    value DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
    'format-version'    = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
);

INSERT INTO interop.shared_mor SELECT * FROM interop.shared_cow VERSION AS OF (
    SELECT MIN(snapshot_id) FROM interop.shared_cow.snapshots
);

DELETE FROM interop.shared_mor WHERE id = 2;
UPDATE interop.shared_mor SET value = 99.0 WHERE id = 3;

SELECT * FROM interop.shared_cow ORDER BY id;
SELECT * FROM interop.shared_mor ORDER BY id;
```

Both tables now logically contain the same 3 rows. Physically they're very different — the MoR table has delete files; the CoW table doesn't. Verify:

```sql
SELECT content, COUNT(*) FROM interop.shared_cow.files GROUP BY content;
SELECT content, COUNT(*) FROM interop.shared_mor.files GROUP BY content;
```

### Step 2 — Read from Trino

```bash
docker compose exec trino trino
```

```sql
SELECT * FROM iceberg.interop.shared_cow ORDER BY id;
SELECT * FROM iceberg.interop.shared_mor ORDER BY id;
```

Both should show 3 rows. Trino in version 455+ handles V2 position deletes and equality deletes correctly. Older Trino versions (< 410) had partial equality-delete support — they returned wrong answers silently.

Inspect what Trino sees of the metadata:

```sql
SHOW CREATE TABLE iceberg.interop.shared_mor;
SELECT * FROM iceberg.interop."shared_mor$files";
```

Trino exposes Iceberg's metadata tables as `<table>$snapshots`, `<table>$files`, `<table>$partitions`. **Different SQL surface than Spark's `<table>.snapshots`**, same underlying data. This dialect difference is the first cross-engine gotcha.

### Step 3 — Read from PyIceberg (the main lab's Jupyter)

In the main lab Jupyter at `http://localhost:8888`:

```python
from pyiceberg.catalog.rest import RestCatalog
import pandas as pd

rest = RestCatalog(
    name="rest_lab",
    **{
        "uri": "http://lakekeeper:8181/catalog",
        "warehouse": "lab",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    },
)

cow = rest.load_table("interop.shared_cow")
mor = rest.load_table("interop.shared_mor")

print("CoW via PyIceberg:")
print(cow.scan().to_arrow().to_pandas())
print()
print("MoR via PyIceberg:")
print(mor.scan().to_arrow().to_pandas())
```

Both should return 3 rows. PyIceberg added equality-delete support around 0.6; if you're on 0.11 you're fine. **The version of your reader library matters as much as the engine.**

### Step 4 — Read from DuckDB (the trickiest one)

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_endpoint='minio:9000';")
con.execute("SET s3_access_key_id='minioadmin';")
con.execute("SET s3_secret_access_key='minioadmin';")
con.execute("SET s3_region='us-east-1';")

cow_loc = cow.location()
mor_loc = mor.location()

print("CoW via DuckDB:")
try:
    print(con.execute(f"SELECT * FROM iceberg_scan('{cow_loc}')").fetchdf())
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")

print("\nMoR via DuckDB:")
try:
    print(con.execute(f"SELECT * FROM iceberg_scan('{mor_loc}')").fetchdf())
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")
```

Expected outcomes (depends on DuckDB iceberg extension version):

- **CoW table**: succeeds, returns 3 rows. CoW = no delete files = every engine can read it.
- **MoR table**:
  - Modern DuckDB iceberg (≥ 0.5): correct, 3 rows
  - Older versions: returns 4 rows (ignored the delete files!)
  - Very old: errors out

**Insight:** the most dangerous failure mode is **silent**. A reader that ignores delete files returns the wrong number of rows without raising an error. If your fleet has a reader you haven't tested, you may have wrong answers in production right now.

### Step 5 — A compatibility test harness

This is what you'd embed in your CI:

```python
def compat_check(table, expected_rows, expected_value_for_id_3):
    """Assert this table reads correctly via this engine."""
    df = table.scan().to_arrow().to_pandas()
    assert len(df) == expected_rows, f"Wrong row count: got {len(df)} expected {expected_rows}"
    row3 = df[df["id"] == 3]
    assert len(row3) == 1
    assert row3.iloc[0]["value"] == expected_value_for_id_3, \
        f"Wrong value for id=3: got {row3.iloc[0]['value']} expected {expected_value_for_id_3}"
    return "OK"

print("PyIceberg / CoW:", compat_check(cow, 3, 99.0))
print("PyIceberg / MoR:", compat_check(mor, 3, 99.0))
```

Run the same logical assertion through every reader (Spark, Trino, DuckDB, PyIceberg). If any reader fails the assertion, you have a compatibility problem.

### Step 6 — What writes can your readers see at all?

A subtle gotcha: the catalog's namespace listing can include tables that some readers can't open. Test the discovery surface:

```python
for ns in rest.list_namespaces():
    for tbl in rest.list_tables(ns):
        try:
            t = rest.load_table(tbl)
            print(f"  {tbl}: format-version={t.metadata.format_version}, snapshots={len(list(t.snapshots()))}")
        except Exception as e:
            print(f"  {tbl}: load FAILED — {type(e).__name__}: {e}")
```

If you see a load failure for a table that Spark created, you've identified a version skew. The Iceberg spec is *forward*-compatible (newer specs can be read by older code if features aren't used), but **only if** features aren't used. A V3 metadata.json that uses no V3-only features may still load on a V2 reader — but if any V3 feature is present, the reader errors.

### Step 7 — Writing from multiple engines

Trino can write Iceberg too, but its CoW/MoR support is more limited than Spark's. Test a write from Trino:

```sql
-- In Trino
INSERT INTO iceberg.interop.shared_cow VALUES (5, 'EU', 50.0, TIMESTAMP '2026-05-18 11:00:00');
DELETE FROM iceberg.interop.shared_cow WHERE id = 1;

SELECT * FROM iceberg.interop.shared_cow ORDER BY id;
```

Then go back to Spark and read:

```sql
-- In Spark
SELECT * FROM rest_lab.interop.shared_cow ORDER BY id;
```

Spark should see the Trino-written rows. **Trino's writes go through the same REST catalog commit path** — atomic pointer swap, no engine-specific bookkeeping. This is exactly the multi-engine promise of Iceberg.

But: try the same against the MoR table.

```sql
-- In Trino
DELETE FROM iceberg.interop.shared_mor WHERE id = 4;
```

Depending on Trino version, this either:
- Succeeds, writes a Trino-flavored delete file (Spark + Trino both read it)
- Falls back to CoW-style rewrite (more expensive but compatible)
- Errors out ("MoR writes not supported in this version")

**Insight:** "multi-engine writes" is much more constrained than "multi-engine reads." If you need writes from multiple engines, **stick to V1-compatible CoW operations** until you've verified each engine's V2 write surface against your spec needs.

---

## 💥 Break it

### Break 1: write a V2-only feature, read from a V1-only reader

```sql
-- Spark
ALTER TABLE interop.shared_mor ADD COLUMNS (extra STRING);
INSERT INTO interop.shared_mor VALUES (10, 'EU', 5.0, TIMESTAMP '2026-05-18 12:00:00', 'new');
```

A reader that's only V1-aware sees the table but is missing the column. **Iceberg's promise:** old readers see the schema they understand (older schema), no crash. **The risk:** they silently drop the new column. Whether that's "OK" depends on your consumer — for a metric pipeline, it might be fine; for an audit report, it might be a compliance issue.

### Break 2: writer A holds an older spec than writer B

If Spark writes a `metadata.json` at format-version 2 with a feature B doesn't understand, B's next commit will *overwrite* the metadata with its own version — possibly downgrading what was there. Lakekeeper's REST API rejects out-of-version writes (the table-requirements array in the commit protocol catches this), but only if writers actually send it. Some Spark + Iceberg combinations don't. **Production rule:** every writer in a multi-writer fleet must be on a compatible Iceberg runtime version.

### Break 3: catalog dialect differences

Spark and Trino expose Iceberg metadata tables differently:

| | Spark | Trino |
|---|---|---|
| Snapshots | `<table>.snapshots` | `<table>$snapshots` |
| Files | `<table>.files` | `<table>$files` |
| Time travel | `VERSION AS OF` / `TIMESTAMP AS OF` | `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` |
| Procedures | `CALL system.expire_snapshots(...)` | `ALTER TABLE … EXECUTE expire_snapshots(...)` |

Code that assumes one dialect breaks in the other. **Insight:** your tooling layer should abstract this, or you should standardize on one dialect for ops queries.

### Break 4: Snowflake / BigQuery / Athena as readers

We can't run these in the lab, but they're the most common production cases. Reality check (as of mid-2026):

| Engine | V1 read | V2 read (data) | V2 read (position deletes) | V2 read (equality deletes) | V2 write |
|---|---|---|---|---|---|
| **Spark + Iceberg runtime** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Trino 455+** | ✅ | ✅ | ✅ | ✅ | partial |
| **PyIceberg 0.7+** | ✅ | ✅ | ✅ | ✅ | partial |
| **DuckDB iceberg ext. 0.5+** | ✅ | ✅ | ✅ | recent | no |
| **Snowflake external tables** | ✅ | ✅ | ✅ | recent | no |
| **AWS Athena** | ✅ | ✅ | ✅ | ✅ | partial |
| **BigQuery BigLake** | ✅ | ✅ | ✅ | partial | no |
| **ClickHouse iceberg engine** | ✅ | ✅ | recent | recent | no |
| **Dremio** | ✅ | ✅ | ✅ | ✅ | partial |

The "recent" column is where production teams get burned. A table that has been pristine for two years suddenly gets a Spark MERGE that writes equality deletes — and a downstream Snowflake external table that was working perfectly starts returning subtle wrong answers. **This is real. It happens. Test before enabling MoR.**

### Break 5: the case for default-CoW

If you operate a multi-engine fleet and you don't have automated cross-engine compat tests, **default every table to copy-on-write**. The write cost is higher but the read surface is universal. Switch to MoR only on tables where:

1. You control all readers, or
2. You've verified all readers handle equality deletes, and
3. Your write volume justifies the cost saving.

This is the answer most "senior" candidates miss — they assume "Iceberg is open" means "I can use any feature." The honest answer is "you can use any feature *that all your readers support*."

---

## 📚 Theory deep-dive

### What "open spec" actually means

The Iceberg spec is a stable document. Any engine claiming Iceberg support implements some version of it. **But:**

- Spec versions evolve. V1 → V2 (2022) → V3 (in progress through 2026).
- Each engine implements a subset.
- Each engine's release schedule differs from the spec's evolution.

So at any moment in time, your engine fleet has a **least-common-feature-set**. Your tables should write what the LCFS supports.

### The compatibility matrix is your job

Most platform teams maintain an internal "what does our Iceberg fleet support" matrix, updated quarterly. Sample:

```
Feature                          | Spark | Trino | DuckDB | Snow | Athena
V2 data files                    |  ✅   |  ✅   |  ✅    |  ✅  |  ✅
V2 position deletes              |  ✅   |  ✅   |  ✅    |  ✅  |  ✅
V2 equality deletes              |  ✅   |  ✅   |  ⚠   |  ⚠  |  ✅
V2 row-level merge writes        |  ✅   |  ⚠   |  ❌    |  ❌  |  ⚠
Sort orders (read)               |  ✅   |  ✅   |  ⚠   |  ✅  |  ✅
Branch / tag reads               |  ✅   |  ⚠   |  ❌    |  ❌  |  ⚠
Bloom filter pushdown            |  ✅   |  ✅   |  ⚠   |  ✅  |  ⚠
```

Update with every engine upgrade. Use it to decide table properties at table-creation time.

### REST catalog as the standardization point

The REST catalog spec made cross-engine interop dramatically better in 2024–2026 — partly because it standardized the auth and credentials surface, and partly because every engine now talks the same protocol to the catalog. Engine-specific Hive/Glue/Sql implementations had subtle differences (column-stats reporting, type mappings, namespace semantics) that REST eliminates.

If you're starting a new multi-engine Iceberg deployment in 2026, **REST catalog is non-negotiable**. The alternative is a maintenance burden you don't need.

### Type system mismatches

Even when engines agree on the spec, types can drift. The Iceberg spec defines its own type system; each engine maps to/from its native types:

- `decimal(38, 18)` in Iceberg maps cleanly to Spark, awkwardly to BigQuery (which has fewer precision options).
- `timestamp` (without zone) vs `timestamptz` — different engines have different defaults.
- Nested struct types — well supported in Spark/Trino, partially in Snowflake external.
- `uuid` — Iceberg-native; some engines map to STRING or BINARY(16).

When designing schemas, test type roundtrips through every reader. This is rare-but-real.

### Schema evolution across engines

Iceberg's schema evolution (experiment 06) is field-id-based, which makes it engine-agnostic. **But:** an engine writing a schema change has to record it correctly. Some engines (older Trino, older Athena) didn't bump the schema version when renaming columns, leading to readers seeing old column names. This was largely fixed in 2025, but legacy tables from earlier may still have inconsistent schema histories.

### The platform team's playbook

1. **Maintain a compatibility matrix**, updated quarterly.
2. **Default tables to CoW** unless a specific MoR justification exists.
3. **Pin engine versions** across the fleet; coordinate upgrades.
4. **CI tests for cross-engine reads** on a canary table — every nightly, write from Spark, read from Trino + DuckDB + PyIceberg, assert equality.
5. **Document engine quirks** in your platform docs — every team will hit them otherwise.

---

## ✍️ Re-answer the interview question

> "Spark writes V2 delete files. What about my Trino / Snowflake / DuckDB / Athena?"

Cover:

1. **Open spec ≠ universal feature parity** — every engine implements a subset.
2. The compatibility matrix is your responsibility as platform team.
3. **Position deletes are widely supported; equality deletes are the long tail.**
4. CoW is the universal-reader safe default; MoR requires you've tested every reader.
5. REST catalog standardizes the catalog surface, not the engine feature surface.
6. Multi-engine writes are much riskier than multi-engine reads — pin engine versions.
7. CI tests for cross-engine reads on canary tables are the production discipline.

---

## 🎁 LinkedIn post draft

> **"'Iceberg is open' got my team into trouble. We wrote V2 equality deletes from Spark and our Snowflake external table started returning wrong numbers — silently. Here's the compatibility matrix I now keep up to date."**
>
> Walk the matrix. End with the default-CoW recommendation.

---

## Session wrap-up

1. Confirm hands-on: you've read the same `interop.shared_cow` and `interop.shared_mor` from Spark, Trino, PyIceberg, DuckDB; you've written from Trino and read from Spark; you've seen the compatibility caveats first-hand.
2. Add to your `interview-faq.md` the answer to "what's the safe default for a multi-engine table?" (CoW + REST catalog + pinned engine versions).
3. Spark profile down when done; main lab stays.

---

## Next up

→ [Experiment 21: Hive → Iceberg migration](21-hive-to-iceberg-migration.md) — the last production gap. Most enterprise Iceberg adoption starts here: a Hive table with 7 years of history that you can't afford to rewrite.
