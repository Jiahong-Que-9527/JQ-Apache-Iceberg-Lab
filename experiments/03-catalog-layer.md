# Experiment 03 — The Catalog Layer

> **Time:** 75 min · **Tier:** Foundation · **Prerequisites:** Experiments 01, 02

---

## 🎯 The interview question this answers

> **"What does a catalog do, and why does Iceberg need one? Which catalog implementation would you choose for a new production deployment and why?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** Catalog choice is the **single most operationally impactful decision** in an Iceberg deployment. Get it wrong and you'll be migrating mid-project. Most candidates can't even name three catalog options. Being able to compare five and justify a choice puts you in the top 10%.

---

## TL;DR

The catalog has one job: **hold the current pointer to the table's metadata.json file, and update it atomically.** That's it. All the variation between SqlCatalog, RestCatalog, GlueCatalog, HiveCatalog, NessieCatalog, and Polaris is about *how* they implement this single atomic-pointer-swap operation — and what extra features they pile on top.

---

## 🛠️ Hands-on: see the catalog with your own eyes

### Step 1 — Open the SQLite catalog directly

You've been using `SqlCatalog` backed by `catalog.db`. Let's see what it actually stores.

```bash
# In a terminal (not the notebook):
sqlite3 catalog.db
```

```sql
.tables
-- You should see: iceberg_namespace_properties, iceberg_tables

.schema iceberg_tables
-- catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, ...

SELECT catalog_name, table_namespace, table_name, metadata_location
FROM iceberg_tables;
```

**Look at the result.** It's literally one row per table, with a path to a metadata.json file. **That's the entire catalog.**

The "catalog" you've been using is a SQL table with one meaningful column: `metadata_location`. Everything else is bookkeeping.

### Step 2 — See an atomic update happen

In the notebook:

```python
from src.catalog_helper import get_catalog
import pyarrow as pa
from datetime import datetime, timezone

catalog = get_catalog()
table = catalog.load_table("lab.events")
print("Before append:", table.metadata_location)
```

Run a write:

```python
data = pa.table({
    "event_id": ["audit_test_1"],
    "event_ts": [datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)],
    "user_id": [999],
    "amount": [42.0],
    "location": [{"country": "DE", "city": "Frankfurt"}],
})
table.append(data)
table = catalog.load_table("lab.events")
print("After append: ", table.metadata_location)
```

The metadata location changed from `v{N}.metadata.json` to `v{N+1}.metadata.json`. 

Now check the SQLite catalog:

```bash
# In terminal:
sqlite3 catalog.db "SELECT metadata_location, previous_metadata_location FROM iceberg_tables WHERE table_name='events';"
```

You'll see the new path in `metadata_location` and the previous one in `previous_metadata_location`. **The actual atomic operation was: `UPDATE iceberg_tables SET metadata_location = ? WHERE ... AND metadata_location = ?` — i.e., a conditional update**. SQLite's transactional guarantee is what makes this atomic.

### Step 3 — Catalog as the single source of truth

Question: what happens if metadata.json files exist in S3, but the catalog doesn't know about them?

```python
# Create a "ghost" table by writing metadata files but not registering with catalog
# We won't actually do this destructively — just reason about it.

# If we ran:
#   s3_client.put_object(bucket="warehouse", key="lab/ghost/metadata/v1.metadata.json", ...)
# without calling catalog.create_table(), then:
#   catalog.list_tables("lab")  -> does NOT include "ghost"
#   catalog.load_table("lab.ghost") -> NoSuchTableError

# Insight: the S3 files are the BODY of the table, but the catalog is the IDENTITY.
# No catalog entry, no table.
```

This is one of the most important catalog properties: **the catalog is the source of truth for "what tables exist."** Object storage is not.

### Step 4 — Migrate to a different catalog (simulated)

Imagine you want to move from SQLite to Postgres. The "data" doesn't move — only the catalog entries.

In a real migration:

```python
# Pseudo-code, not run:

# 1. Read all tables from the old catalog
old_catalog = SqlCatalog("old", uri="sqlite:///catalog.db", ...)
tables = old_catalog.list_tables("lab")

# 2. For each table, register the SAME metadata_location in the new catalog
new_catalog = SqlCatalog("new", uri="postgresql://...", ...)
for namespace, table_name in tables:
    old_table = old_catalog.load_table(f"{namespace}.{table_name}")
    new_catalog.register_table(
        identifier=f"{namespace}.{table_name}",
        metadata_location=old_table.metadata_location,
    )
```

**Three implications:**
- Catalog migration is fast (it's just rewriting pointers)
- No data is copied
- All clients must switch at once (you can't have two catalogs disagreeing about a table's current state)

### Step 5 — Try RestCatalog (optional, advanced)

If you want to see a "real" production-style catalog, run a quick Polaris or Lakekeeper container alongside MinIO. This is optional and adds 30 minutes. The PyIceberg API to use it:

```python
# Pseudo-code — see the iceberg-lab repo for an optional docker-compose.rest.yml
from pyiceberg.catalog.rest import RestCatalog

rest_catalog = RestCatalog(
    name="prod-like",
    uri="http://localhost:8181/api/catalog",
    warehouse="s3://warehouse/",
    # token=..., credentials=...
)

# The API is identical to SqlCatalog. That's the point.
```

**Insight:** the catalog interface is **stable across implementations**. This is by design — the Iceberg REST Catalog Spec exists to make catalogs swappable.

---

## 💥 Break it

### Break 1: race conditions

Open two notebook kernels (or two terminals). In both, do:

```python
from src.catalog_helper import get_catalog
import pyarrow as pa
from datetime import datetime, timezone

catalog = get_catalog()
table = catalog.load_table("lab.events")
data = pa.table({...})  # some data
```

Now in BOTH at the same time, run `table.append(data)`.

What happens? With SQLite, one will succeed and the other will get a `CommitFailedException` due to the conditional update failing. **Insight:** this is **optimistic concurrency control**. The catalog enforces it via the conditional update. We'll explore this deeply in Experiment 08.

### Break 2: delete the catalog entry but keep S3 files

```python
catalog.drop_table("lab.events")
```

Now look at MinIO. The `lab/events/` folder still exists, with all metadata and data files. **But the table is gone** as far as the catalog is concerned.

```python
catalog.load_table("lab.events")  # NoSuchTableError
```

This is **expected behavior**. By default `drop_table` does NOT delete S3 files (you can pass `purge_requested=True` if you want that). **Insight:** "soft delete" is the default. This matters for accidental drops and for compliance scenarios where data retention is mandatory.

To recover:

```python
catalog.register_table(
    identifier="lab.events",
    metadata_location="s3://warehouse/lab/events/metadata/v{LATEST}.metadata.json"
    # You'd need to know the latest version, e.g., from S3 LIST
)
```

Now the table is back. **The S3 files were never the table; the catalog entry was the table's identity.**

---

## 📚 Theory deep-dive

### The five catalog families

| Catalog | Storage | Pros | Cons | Use when |
|---|---|---|---|---|
| **SqlCatalog** (SQLite/Postgres) | A SQL database | Simple, dev-friendly, you control it | You operate the DB | Small teams, on-prem, lots of control needed |
| **HiveCatalog** | Hive Metastore | Compatible with legacy Hive tools | Hive operational tax, no built-in branching | Migrating from Hive |
| **GlueCatalog** | AWS Glue | AWS-native, IAM integration | AWS lock-in, slow updates | Pure AWS shop |
| **RestCatalog** | Any service implementing the REST spec | Vendor-neutral API | Need to run the service | Multi-engine, multi-cloud |
| **NessieCatalog** | Project Nessie | Git-like branching, multi-table transactions | Less ecosystem support | Data versioning needs |
| **Polaris** (Snowflake OSS) | REST-based | Strong IAM, growing adoption | Newer, less mature | Snowflake/AWS deployments |
| **Lakekeeper** | REST-based, open-source | OIDC/keycloak integration | Smaller community | Self-hosted EU FinTech (good DORA story) |

### Why catalog choice is hard

The right answer to "which catalog?" depends on:

1. **Where are you running?** AWS → Glue/Polaris is easy. Self-hosted EU → Lakekeeper/Nessie/Postgres-backed REST.
2. **Who writes to the tables?** Multiple engines (Spark + Trino + Flink + Python) → you need REST or a metastore that all of them support.
3. **What's your operational model?** Managed service team → REST catalog. DBA-driven → SQL catalog.
4. **Do you need branching / multi-table transactions?** → Nessie or Polaris.
5. **What's your compliance/security model?** Self-hosted with audit logging → Lakekeeper. AWS IAM-integrated → Glue or Polaris.

**The wrong answer in an interview:** "We just use Glue because it's the default." This shows you didn't think.

**The right answer:** "For DBG's use case [internal compute on GCP + Azure with Databricks], I'd evaluate three options: Unity Catalog because they already pay for Databricks; Polaris if they want vendor neutrality; Lakekeeper if they want a self-hosted OSS option with strong IAM. Tradeoffs are [X, Y, Z]." **This is the answer that gets the senior offer.**

### Atomic commits — the implementation reality

Different catalogs implement the "atomic pointer swap" differently:

- **SQLite/Postgres:** `UPDATE iceberg_tables SET metadata_location = ? WHERE name = ? AND metadata_location = ?` — relies on RDBMS transaction.
- **HiveCatalog:** uses Hive Metastore's table parameter update with a lock.
- **GlueCatalog:** uses Glue's `UpdateTable` with version checks.
- **RestCatalog:** uses HTTP `POST /tables/.../commit` with the previous metadata location in the request body.
- **NessieCatalog:** commits to a Git-style branch ref.

The interface is the same. The mechanism is engine-specific. **Most candidates can't explain this. You will.**

---

## 🇪🇺 Regulatory angle

For EU FinTech deployments, three catalog properties matter regulatorily:

1. **Auditability of catalog operations.** Who created/dropped which table when? GlueCatalog has CloudTrail. Lakekeeper has built-in audit logs. Custom SqlCatalog needs you to wrap operations in your own audit logging. **DORA Article 9** requires this kind of traceability.

2. **Data sovereignty.** If you're a German bank with data residency obligations, your catalog must run in EU. This rules out catalogs that phone home to non-EU SaaS endpoints. **Lakekeeper, Postgres-backed REST catalogs, and Nessie are EU-friendly.**

3. **IAM integration.** **BaFin BAIT** expects strong access controls. Glue integrates with IAM. Lakekeeper integrates with OIDC/Keycloak. NessieCatalog has its own ACL model. SqlCatalog has nothing built-in — you must enforce at the network layer.

If asked "what catalog would you recommend for a Frankfurt-based bank?" — **Lakekeeper or a Postgres-backed REST catalog with EU-hosted dependencies** is a defensible answer. You sound like someone who has actually thought about the EU regulatory context, which is rare.

---

## ✍️ Re-answer the interview question

> "What does a catalog do, and which would you choose for a new production deployment?"

Your answer should cover:

1. The catalog's single job: atomic pointer to current metadata
2. The catalog is the source of truth for table identity (S3 files alone don't define a table)
3. Trade-offs between the five families
4. A concrete recommendation conditional on the deployment context (cloud, team, compliance, multi-engine needs)

Aim for 90 seconds. Practice it.

---

## 🎁 LinkedIn post draft

> **"I spent an evening comparing five Iceberg catalog implementations. Here's the decision framework I wish I had when I started."**
>
> The table from the theory section, plus your own opinion on which fits which use case. **EU readers especially will engage with the Lakekeeper/regulatory angle** — that's a niche almost nobody is covering.

---

## Next up

→ [Experiment 04: Reads without Spark](04-reads-without-spark.md) — explore how Iceberg is consumed from Python, DuckDB, Trino. We'll measure latency and discuss when Spark is necessary vs overkill.
