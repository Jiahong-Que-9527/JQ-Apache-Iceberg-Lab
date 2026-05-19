# Experiment 16 — Production Catalog: REST Catalog & Lakekeeper

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Experiments 01–03 · **Spark profile required** (for the Lakekeeper REST catalog)

---

## 🎯 The interview question this answers

> **"Why would you choose a REST catalog (Lakekeeper / Polaris / Tabular / Gravitino) over SqlCatalog, HiveCatalog, or GlueCatalog for production? What is vended credentials? What breaks when you migrate catalogs?"**

Pause. Open `interview-faq.md`. Write your current answer. If your answer is "REST has an HTTP API," it's not wrong but it's missing 90% of the point.

**Why this matters:** Catalog choice is the **single most operationally impactful decision** in an Iceberg deployment (we said this back in experiment 03). REST catalog is the answer that's emerged as production-default by 2026 — but most candidates can't articulate *why*. The "why" is what gets you the senior offer.

---

## TL;DR

The Iceberg **REST Catalog Spec** is an open HTTP protocol that any engine can speak. A REST catalog server (Lakekeeper, Polaris, Tabular, Gravitino) acts as the **single source of truth** for table metadata pointers — *and* is the only place where authorization, vended credentials, and audit can be cleanly centralized. SqlCatalog/HiveCatalog/GlueCatalog all leak some of that responsibility to the client. REST owns the whole boundary.

By the end of this experiment you'll have:
- The same lab table living in three catalogs (SqlCatalog, REST/Lakekeeper, and migrated between them)
- Hands-on with the REST API directly (no PyIceberg in the way)
- A clear model of what a "catalog migration" actually moves (almost nothing — that's the punchline)

---

## 🛠️ Hands-on: see the REST catalog with your own eyes

### Step 0 — Bring up the Spark profile

```bash
# Main lab up first
cd lab && ./init.sh

# Spark profile (Lakekeeper + Spark + Trino)
cd spark-profile
./up.sh
```

Verify Lakekeeper is healthy:

```bash
curl -s http://localhost:8181/health
```

### Step 1 — Look at the REST API directly

The REST Catalog spec is small. Here's the surface, raw:

```bash
# List namespaces in the bootstrapped 'lab' warehouse
curl -s http://localhost:8181/catalog/v1/lab/namespaces | jq .

# Create a namespace
curl -s -X POST http://localhost:8181/catalog/v1/lab/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace": ["demo16"], "properties": {"owner": "lab"}}' | jq .

# List tables in a namespace
curl -s http://localhost:8181/catalog/v1/lab/namespaces/demo16/tables | jq .
```

You just spoke the REST Catalog protocol with curl. Any engine — Spark, Trino, DuckDB, PyIceberg, Snowflake, BigQuery — can do the same. **This is the whole point.**

### Step 2 — Create a table via PyIceberg → REST → Lakekeeper

From the main lab's Jupyter (`http://localhost:8888`):

```python
import os
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType
import pyarrow as pa
from datetime import datetime, timezone

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

# Namespaces
print("Namespaces:", rest.list_namespaces())

rest.create_namespace_if_not_exists("demo16")

schema = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=True),
    NestedField(3, "amount",   LongType()),
)

if ("demo16", "events") in [tuple(t) for t in rest.list_tables("demo16")]:
    rest.drop_table("demo16.events")

table = rest.create_table("demo16.events", schema=schema)
table.append(pa.table({
    "event_id": ["a","b","c"],
    "event_ts": [datetime.now(timezone.utc)]*3,
    "amount":   [10, 20, 30],
}))

print("Table location:", table.location())
print("Rows:", table.scan().to_arrow().num_rows)
```

You just wrote an Iceberg table whose **catalog state lives in Lakekeeper's Postgres**, **data files live in MinIO**, and **PyIceberg never directly opened a SQLite file**. That's the production topology.

### Step 3 — Read the same table from Spark and Trino

Spark:

```bash
docker compose exec spark-iceberg spark-sql
```

```sql
SELECT * FROM rest_lab.demo16.events;
```

Trino:

```bash
docker compose exec trino trino
```

```sql
SELECT * FROM iceberg.demo16.events;
```

Both engines hit the **same Lakekeeper REST endpoint**, the **same metadata.json**, the **same Parquet files**. Three engines, one catalog, one storage. This is what "open table format" actually means.

### Step 4 — Authentication: the part that matters in production

So far Lakekeeper is running with `LAKEKEEPER__AUTHZ_BACKEND=allow-all`. That's lab-only. In production you'd front it with OAuth2/OIDC and per-warehouse RBAC. Inspect what an authenticated request looks like (even if we accept all tokens):

```bash
# Production-shaped request with a bearer token
curl -s http://localhost:8181/catalog/v1/lab/namespaces \
  -H 'Authorization: Bearer some.opaque.token' | jq .
```

The REST spec defines `OAuth2 client credentials` and `token exchange` flows. **The catalog is the authentication boundary** — your engines hold tokens; the catalog decides what they can read or write. In SqlCatalog, every client has the database password. In HiveCatalog, every client has direct HMS thrift access. In GlueCatalog, every client has an IAM role with broad Glue permissions. **REST is the first design where the catalog can enforce per-table access without trusting the client.**

### Step 5 — Vended credentials (the killer feature)

Vended credentials are the answer to the question *"if my engines hold S3 credentials, what's the security boundary?"*

Without vended creds (classic Spark/Glue setup):
1. Spark holds long-lived AWS access keys with `s3:GetObject` / `s3:PutObject` on the bucket
2. Spark asks the catalog for `metadata_location`
3. Spark reads/writes S3 directly with its own keys
4. **The catalog cannot stop Spark from reading any object in the bucket**, including other tenants' tables. Bucket policies are the only fence.

With vended creds (REST + Lakekeeper / Polaris):
1. Spark asks the catalog "load table X"
2. **Catalog mints short-lived (15 min) S3 credentials scoped to X's prefix** and returns them in the response
3. Spark uses those creds to read/write S3
4. **The catalog is the policy point.** Tenant isolation, row-prefix isolation, audit trail — all centralized.

Lakekeeper supports this via STS-style key minting. In the lab we're using static MinIO creds (because MinIO's STS support is partial), but the API shape is the same. Look at the table-load response:

```bash
curl -s http://localhost:8181/catalog/v1/lab/namespaces/demo16/tables/events | jq '.config'
```

In a production deployment that response would include `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, and an expiration. Your engine uses those *for this load only*.

**This is why every major cloud Iceberg vendor in 2026 ships a REST catalog.** It's the only architecture where you can sell "Iceberg as a managed service" without giving customers your bucket keys.

### Step 6 — Migrate `lab.events` (SqlCatalog) → REST catalog

In the main lab, `lab.events` lives in SqlCatalog. Move its registration to Lakekeeper without rewriting data.

```python
from src.catalog_helper import get_catalog

sql = get_catalog()                  # SqlCatalog
src_table = sql.load_table("lab.events")
metadata_loc = src_table.metadata_location
print("Current metadata_location:", metadata_loc)
```

The migration is one HTTP call: tell Lakekeeper "register this existing metadata.json under namespace X / table Y."

```python
import requests
rest_uri = "http://lakekeeper:8181/catalog"

rest.create_namespace_if_not_exists("migrated")

resp = requests.post(
    f"{rest_uri}/v1/lab/namespaces/migrated/register",
    json={
        "name": "events",
        "metadata-location": metadata_loc,
    },
)
print(resp.status_code, resp.text[:500])

# Confirm via PyIceberg-REST
print("REST tables:", rest.list_tables("migrated"))
mig = rest.load_table("migrated.events")
print("Migrated table rows:", mig.scan().to_arrow().num_rows)
```

**Look at what we moved:** zero data files. Zero metadata files. Only a row in Lakekeeper's Postgres pointing at the existing `metadata.json` in MinIO. **The catalog migration is just re-registration.** Most teams overestimate the work involved by a factor of 10.

(In production you'd also de-register the old SqlCatalog row so both don't claim ownership. We'll leave both registered here for inspection.)

### Step 7 — What happens if both catalogs commit?

Both catalogs now believe they own the table. Try a write through SqlCatalog:

```python
sql_table = sql.load_table("lab.events")
sql_table.append(pa.table({
    "event_id": ["zzz"],
    "event_ts": [datetime.now(timezone.utc)],
    "user_id":  [999],
    "amount":   [1.0],
    "location": [{"country": "DE", "city": "Frankfurt"}],
}))
```

Now refresh through REST:

```python
mig = rest.load_table("migrated.events")
print("REST sees rows:", mig.scan().to_arrow().num_rows)
```

Depending on caching it may or may not see the new row immediately. **Insight:** two catalogs pointing at the same table is a **split-brain** waiting to happen. They'll race on the `metadata_location` pointer. One will succeed; the other will think it's still on the old version and reject its next commit as a conflict — or, worse, silently overwrite if they happen to align.

**Production rule:** one table = one catalog. **Always.** When you migrate, you migrate fully — old catalog gets the row deleted or the table dropped (without dropping the data, see below).

### Step 8 — Drop from old catalog without losing data

```python
sql.drop_table("lab.events", purge_requested=False)
print("Sql catalog tables:", list(sql.list_tables("lab")))
print("REST still has the table:", rest.list_tables("migrated"))
```

`purge_requested=False` is critical — it tells the catalog "remove the catalog row, do NOT delete the underlying data/metadata files in S3." The REST catalog's pointer is now the only one, and the data is intact.

**The mental model:** the catalog row is a label. The metadata.json tree is the actual table. Dropping the label without `purge` is the safe way to switch ownership.

---

## 💥 Break it

### Break 1: stop Lakekeeper mid-commit

Open a long write loop in PyIceberg:

```python
import threading, time
def write_loop():
    t = rest.load_table("demo16.events")
    for i in range(50):
        try:
            t.append(pa.table({"event_id":[f"x{i}"], "event_ts":[datetime.now(timezone.utc)], "amount":[i]}))
            print("commit", i, "ok")
        except Exception as e:
            print("commit", i, "failed:", type(e).__name__, str(e)[:160])
            break
        time.sleep(0.2)
threading.Thread(target=write_loop, daemon=True).start()
```

In another terminal:

```bash
docker compose stop lakekeeper
sleep 2
docker compose start lakekeeper
```

You'll see the commit loop error out mid-stream, then resume after Lakekeeper recovers. **Insight:** the catalog is a *blocking* dependency for writes. Plan for its HA accordingly. Lakekeeper itself is stateless behind Postgres — Postgres HA is the real question. In production: managed Postgres with read replicas, or RDS Multi-AZ.

### Break 2: bypass the catalog and edit S3 directly

In MinIO, take a copy of one of your metadata.json files, replace it under a different name, and ask Lakekeeper to register *that* version of the table. Now the catalog points at a metadata version that doesn't match the data state.

```python
# Conceptual — the API is open, the consequence is yours
# rest.update_table(metadata_location="s3://warehouse/.../bogus.metadata.json")
```

Most REST catalogs will accept the registration without validating. **Insight:** the catalog trusts you, the operator, to tell it the truth about which metadata.json represents the current state. Garbage in, garbage out. A common production scar: a cleanup job that deleted the "wrong" metadata.json forced operators to walk the snapshot tree by hand to recover.

### Break 3: catalog auth misconfiguration

Try a write with a token that the catalog rejects (in a properly secured setup; in our lab `allow-all` accepts everything):

```bash
# In a real Lakekeeper with OIDC:
# curl -X POST .../namespaces/foo/tables -H 'Authorization: Bearer wrong'
# → 401 Unauthorized
```

The Spark/Trino/PyIceberg behaviour on a 401 is to fail the commit cleanly. The data files Spark wrote *before* the commit may still be sitting in S3 — **the catalog's reject left orphan files**. This is the most common "why is my S3 bill rising" mystery in production. Orphan file cleanup (experiment 10) is what catches these.

### Break 4: catalog as a SPOF

Take down Lakekeeper entirely:

```bash
docker compose stop lakekeeper lakekeeper-db
```

Now try to read from Trino:

```sql
SELECT * FROM iceberg.demo16.events;
```

Trino errors out. **Reads need the catalog too** — because reads have to ask "what is the current metadata_location?" The data is fine, sitting in S3, but with no catalog there's no entry point. Some engines cache metadata locations briefly; once the cache expires, reads fail.

**Production rule:** the catalog's availability is the table's availability. Architect accordingly.

Bring it back:

```bash
docker compose start lakekeeper-db lakekeeper
```

---

## 📚 Theory deep-dive

### The catalog implementation matrix

| Catalog | Where state lives | Auth model | Vended creds | Multi-tenant | Production today (2026) |
|---|---|---|---|---|---|
| **SqlCatalog** (SQLite/Postgres) | a relational table | client holds DB credentials | no | no | dev / single-team prod |
| **HiveCatalog** | Hive Metastore (Thrift) | Kerberos, sentry, or none | no | partial | legacy Hadoop migrations |
| **GlueCatalog** (AWS) | AWS Glue | IAM | partial (via assume-role) | per-account | AWS-only shops |
| **Nessie** | Nessie server (Postgres-backed) | OIDC | no (Iceberg-side) | yes | data versioning use cases |
| **REST (Lakekeeper / Polaris / Tabular / Gravitino)** | REST server + its own DB | OAuth2 / OIDC | **yes** | yes | **production default** |

The 2024–2026 shift to REST catalogs happened for one reason: **vended credentials + per-table authz**. Everything else (open spec, multi-engine, language-agnostic clients) was already true of other catalogs.

### The REST Catalog Spec in one paragraph

GET/POST/DELETE on resources like `/v1/{prefix}/namespaces`, `/v1/{prefix}/namespaces/{ns}/tables`, `/v1/{prefix}/namespaces/{ns}/tables/{table}`. Commits are atomic via `POST .../tables/{table}` with a `requirements` array (e.g., "must currently be at this metadata-location"). The response includes the new `metadata_location` and optionally vended S3 credentials. Spec: <https://iceberg.apache.org/spec/#rest>.

### How vended credentials shape the security model

Without REST + vended creds, your S3 bucket policies are the *only* enforcement layer. You end up writing IAM policies like "engineering team can read prefixes A and B, finance team can read prefixes C and D" — managed in cloud IAM, completely outside Iceberg.

With REST + vended creds, the same policy lives in the catalog. The catalog answers `loadTable` with creds *scoped to that table's prefix only*, valid for 15 minutes. The bucket policy degenerates to "any STS principal minted by Lakekeeper can read this bucket." Lakekeeper does the rest.

This collapses two policy systems (catalog + cloud IAM) into one (catalog). Engineers stop arguing with platform about IAM tickets. Auditors get one place to look.

### Catalog migration: what actually moves

| | Stays | Moves |
|---|---|---|
| Data files (Parquet) | yes, in S3 | no |
| Manifest files / manifest lists | yes, in S3 | no |
| metadata.json | yes, in S3 | no |
| The catalog row pointing at metadata.json | no | yes — that's the entire migration |

So the migration is: "delete from `old_catalog.iceberg_tables`, insert into `new_catalog.tables` pointing at the same metadata location." Tens of milliseconds per table. **The hard part is the cutover**: ensuring no writer is still using the old catalog when the new one takes over. The standard playbook:

1. Freeze writes
2. Register in new catalog (`register_table` / `register` API)
3. Point all writers at the new catalog (config rollout)
4. Drop without purge from old catalog
5. Unfreeze

In a properly architected platform, step 3 is a config flag flip — minutes. The freeze window in step 1 is what matters operationally.

### When NOT to use REST

- **You have one engine and one team**: SqlCatalog or GlueCatalog is fine. REST's auth machinery is overhead.
- **Your environment forbids HTTP between engines and catalogs**: rare but real (air-gapped). HiveCatalog over Thrift is the fallback.
- **You haven't yet thought about authorization**: REST shines when you have multi-tenant requirements. If you don't, the simpler catalogs work.

### Trade-offs vs Nessie

Nessie is also "a server in front of metadata pointers" but it adds Git-style branching at the *catalog* level (not the table level). Confusingly, Iceberg tables themselves now have branches (experiment 17) — so a lot of what Nessie offered (cross-table transactions, branch isolation) can now be partially done within Iceberg. Nessie remains attractive if you want **multi-table atomic commits** ("either all five tables update or none do"). Standard REST catalogs don't do this; the Iceberg spec doesn't either.

In an interview: "We picked Lakekeeper/Polaris because we needed per-table RBAC and vended creds. We considered Nessie but our consistency needs were per-table — we use Iceberg branches for atomic-per-table WAP, and didn't need cross-table transactions."

---

## ✍️ Re-answer the interview question

> "Why REST catalog over Sql/Hive/Glue? What is vended credentials?"

Cover:

1. **REST catalog spec**: open HTTP protocol, same wire format for every engine.
2. **Auth boundary**: the catalog is where authentication and authorization live; previous catalogs leak this to client config.
3. **Vended credentials**: the catalog mints short-lived, table-scoped S3 creds; engines never hold long-lived bucket keys.
4. **Multi-tenant**: one Lakekeeper/Polaris fronts many warehouses, each with isolated namespaces.
5. **Migration is cheap**: re-register the metadata.json, nothing moves.
6. **The cost**: catalog is now a hot dependency, including for reads — design for HA on its backing store.

---

## 🎁 LinkedIn post draft

> **"I migrated an Iceberg table from SqlCatalog to a REST catalog in 50 milliseconds. Zero data moved. The story behind that one HTTP call is the whole reason production Iceberg has converged on REST in 2026."**
>
> Walk through what the catalog actually owns (a pointer), the vended-creds security shift, and the operational cost (the catalog is now your hot path).

This post will out-perform almost anything else you've written. The vended-creds angle is something most engineers haven't been told clearly.

---

## Session wrap-up

1. Confirm hands-on: `demo16.events` exists in Lakekeeper, `migrated.events` is the re-registered version of `lab.events`, you've read the same data from Spark/Trino/PyIceberg through one REST endpoint.
2. Update `interview-faq.md`.
3. Reset / shutdown:
   - End of day: `cd lab/spark-profile && ./down.sh` (keeps Lakekeeper Postgres state), then `cd lab && docker compose stop`.
   - Full cleanup: `./down.sh && rm -rf state/`, then main lab §10.

---

## Next up

→ [Experiment 17: Write-Audit-Publish with branches & tags](17-wap-branches-tags.md) — now that you have a real production catalog, time to use the feature that catalog-plus-branches enables: data quality gates between writers and readers.
