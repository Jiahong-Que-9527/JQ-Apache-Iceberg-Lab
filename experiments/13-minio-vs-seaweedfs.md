# Experiment 13 — MinIO vs SeaweedFS for Iceberg

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Tier 1 complete; Experiment 09 recommended (small files)

---

## 🎯 The interview question this answers

> **"Iceberg tables sit on S3-compatible object storage. How would you choose MinIO vs SeaweedFS? What does Iceberg require from the storage layer?"**

Pause. Write your current answer in `interview-faq.md`.

**Why this matters:** Senior platform interviews rarely stop at table formats. They probe whether you understand **what happens below the catalog** — LIST cost, commit latency, consistency, and operational tradeoffs. This experiment makes those costs measurable on your laptop.

---

## TL;DR (read this last)

MinIO and SeaweedFS both expose an S3 API, so **PyIceberg and DuckDB see the same Iceberg metadata tree** on either backend. The differences show up in **operations and scale**: how many objects you create per commit, how expensive `ListObjectsV2` becomes, and how much complexity you accept to run the cluster. Iceberg does not care about the brand name — it cares about **correct S3 semantics** and **acceptable LIST/read performance** for your snapshot and small-file profile.

---

## 🛠️ Hands-on: same Iceberg workload, two backends

Experiments 01–12 use **MinIO only**. This experiment starts **both** MinIO and SeaweedFS (see [`docs/operation-guide.md`](../docs/operation-guide.md)).

### Step 0 — Start the dual-backend lab

```bash
cd lab
./init.sh
```

Open:

- JupyterLab: http://localhost:8888
- MinIO console: http://localhost:9001 (`minioadmin` / `minioadmin`)
- SeaweedFS master UI: http://localhost:9333
- SeaweedFS S3 API (from host): http://localhost:8333 (`seaweedadmin` / `seaweedadmin`)

Run [`lab/notebooks/08_storage_backends.ipynb`](../lab/notebooks/08_storage_backends.ipynb) or follow the steps below.

### Step 1 — Health check both backends

```python
from src.catalog_helper import get_catalog, get_s3_client, ensure_namespace, storage_console_url

for backend in ["minio", "seaweed"]:
    s3 = get_s3_client(backend)
    print(backend, [b["Name"] for b in s3.list_buckets()["Buckets"]])
    catalog = get_catalog(backend)
    ensure_namespace(catalog)
    print("  console:", storage_console_url(backend))
```

**Insight:** you now have **two SQLite catalogs** — `catalog.db` (MinIO) and `catalog_seaweed.db` (SeaweedFS). Same PyIceberg API, different `s3.endpoint` and warehouse binding.

### Step 2 — Mirror table on each backend

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestampType
import pyarrow as pa
from datetime import datetime, timezone

from src.catalog_helper import get_catalog, ensure_namespace

schema = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=True),
    NestedField(3, "user_id", LongType()),
    NestedField(4, "amount", DoubleType()),
)

def batch(n: int, rows: int = 10):
    now = datetime.now(timezone.utc)
    return pa.table({
        "event_id": [f"{n}-{i}" for i in range(rows)],
        "event_ts": [now] * rows,
        "user_id": list(range(rows)),
        "amount": [float(i) for i in range(rows)],
    })

tables = {}
for backend in ["minio", "seaweed"]:
    catalog = get_catalog(backend)
    ensure_namespace(catalog)
    try:
        catalog.drop_table("lab.storage_compare")
    except Exception:
        pass
    t = catalog.create_table("lab.storage_compare", schema=schema)
    for i in range(3):
        t.append(batch(i))
    tables[backend] = catalog.load_table("lab.storage_compare")
    print(backend, tables[backend].location())
```

Browse both consoles under `warehouse/lab/storage_compare/`. You should see the same **four-layer Iceberg layout** (metadata → manifest list → manifests → Parquet) from Experiment 01.

### Step 3 — Compare object counts

```python
from src.catalog_helper import list_object_keys

for backend, table in tables.items():
    prefix = table.location().replace("s3://", "").split("/", 1)[1].rstrip("/") + "/"
    keys = list_object_keys(prefix, backend=backend)
    meta = [k for k in keys if "/metadata/" in k]
    data = [k for k in keys if "/data/" in k]
    print(backend, "objects:", len(keys), "metadata:", len(meta), "data:", len(data))
```

Record the numbers. Small differences are normal (UUID paths, timing). **Structure** should match.

### Step 4 — Read with PyIceberg and DuckDB

```python
import duckdb
from src.catalog_helper import configure_duckdb_for_s3

for backend, table in tables.items():
    n_py = table.scan().to_arrow().num_rows
    con = duckdb.connect()
    configure_duckdb_for_s3(con, backend)
    n_duck = con.execute(
        f"SELECT count(*) FROM iceberg_scan('{table.location()}')"
    ).fetchone()[0]
    print(backend, "pyiceberg", n_py, "duckdb", n_duck)
```

Both engines should return **30 rows** (3 appends × 10 rows). If one backend fails here, note the exact error — that is interview gold for “S3 compatibility gaps.”

### Step 5 — Small-file storm (connects to Experiment 09)

Run the storm section in notebook 08 (`STORM_COMMITS = 200`, one row per commit). Fill in this table from your run:

| backend | commits | data_files | write_sec | list_sec | total_objects |
|---------|---------|------------|-----------|----------|---------------|
| minio   | 200     | ?          | ?         | ?        | ?             |
| seaweed | 200     | ?          | ?         | ?        | ?             |

**What to look for:**

- `data_files` should be ≈ 200 on both (Iceberg created one file per commit).
- `list_sec` is a crude proxy for **planning pain** — Iceberg must LIST metadata prefixes during commits and scans.
- `write_sec` includes commit + upload; compare backends but treat absolute numbers as lab-only.

### Step 6 — Operations walkthrough

| Task | MinIO | SeaweedFS |
|------|-------|-----------|
| Browse objects | Console :9001 → bucket `warehouse` | Master UI :9333 + filer paths under volume data |
| Credentials | `minioadmin` | `seaweedadmin` (see `lab/seaweedfs/s3-config.json`) |
| Catalog file | `lab/catalog.db` | `lab/catalog_seaweed.db` |
| PyIceberg entry | `get_catalog("minio")` | `get_catalog("seaweed")` |

In production you also watch: LIST requests/sec, commit p99, storage growth per snapshot, and orphan file rate after expiration (Experiment 10).

---

## 💥 Break it

### Break 1 — Cross-wired endpoint (catalog vs storage mismatch)

Create a table on SeaweedFS, then try to read it with MinIO's S3 client using the metadata location from Seaweed:

```python
from src.catalog_helper import get_catalog, read_json_s3

seaweed_table = get_catalog("seaweed").load_table("lab.storage_compare")
meta_uri = seaweed_table.metadata_location  # or current_metadata_location(table)

# This uses MinIO credentials/endpoint — wrong backend for Seaweed paths
try:
    read_json_s3(meta_uri, backend="minio")
except Exception as e:
    print(type(e).__name__, e)
```

**Insight:** the **catalog + endpoint + credentials** must match where objects actually live. Mixed wiring is a common “Iceberg is broken” incident after migrations.

**Reset:** `./reset.sh --confirm`

### Break 2 — Delete a data file in the console

On **one** backend, open `warehouse/lab/storage_compare/data/` and delete a `.parquet` file.

```python
table = get_catalog("seaweed").load_table("lab.storage_compare")  # or minio
try:
    print(table.scan().to_arrow())
except Exception as e:
    print(type(e).__name__, e)
```

Same lesson as Experiment 01: metadata still claims rows exist; storage contract is violated.

**Reset:** `./reset.sh --confirm`

### Break 3 — Wipe storage but keep the catalog

Stop the lab, then from `lab/`:

```bash
docker compose down
rm -rf warehouse-seaweed   # only Seaweed data; leave catalog_seaweed.db
docker compose up -d minio seaweedfs
docker compose up seaweed-bucket-init
docker compose up -d jupyter
```

```python
from src.catalog_helper import get_catalog
catalog = get_catalog("seaweed")
catalog.load_table("lab.storage_compare")  # may load metadata pointer
table = catalog.load_table("lab.storage_compare")
table.scan().to_arrow()  # likely fails — objects gone
```

**Insight:** dropping the catalog entry is not the same as deleting S3 data (Experiment 03). Here you did the opposite — **orphaned catalog pointer, empty storage**.

Recovery path (if you know the latest `metadata.json` path): `catalog.register_table(...)`. Full reset is `./reset.sh --confirm`.

### Break 4 — Wrong credentials (compare error messages)

```python
import duckdb
from src.catalog_helper import configure_duckdb_for_s3

for backend in ["minio", "seaweed"]:
    con = duckdb.connect()
    configure_duckdb_for_s3(con, backend)
    con.execute("SET s3_access_key_id='wrong';")
    try:
        loc = get_catalog(backend).load_table("lab.storage_compare").location()
        con.execute(f"SELECT count(*) FROM iceberg_scan('{loc}')").fetchone()
    except Exception as e:
        print(backend, type(e).__name__, str(e)[:120])
```

Recognize these errors now — most production “Iceberg” tickets are storage auth or endpoint misconfiguration.

---

## 📚 Theory deep-dive

### What Iceberg needs from object storage

Iceberg's **catalog** provides the atomic pointer swap for commits. Object storage must still support:

| Capability | Why Iceberg cares |
|------------|-------------------|
| `PutObject` / multipart upload | Write data and metadata files |
| `GetObject` | Read manifests and Parquet |
| `ListObjectsV2` | Find metadata versions, manifests, data files; planning walks prefixes |
| Consistent read-after-write for commits | Readers must not see partial commits; writers retry on conflict |
| Delete / bulk delete (for maintenance) | Expire snapshots, remove orphans (Experiment 10) |

Production **AWS S3** (with strong consistency since 2020) remains the reference. MinIO and SeaweedFS are approximations — validate your workload.

### MinIO vs SeaweedFS (interview table)

| Dimension | MinIO | SeaweedFS |
|-----------|-------|-----------|
| Architecture | Single-process S3 server; erasure coding on disks | Master + volume servers + filer + S3 gateway |
| Sweet spot | Dev/prod “S3 in your DC”; broad tooling compatibility | Very large object counts; small-file-heavy workloads |
| Iceberg friction | LIST cost grows with snapshots + small files (exp 09) | Same LIST dynamics; measure on your cluster |
| Ops complexity | Low for one node | Higher (more moving parts) |
| Licensing note | Check MinIO edition/license for your deployment | Apache 2.0 |

Neither replaces **compaction** or **snapshot expiration** — they store what Iceberg writes.

### When to choose which

**Choose MinIO** when you want the closest “private S3” experience, minimal moving parts for a team lab, or alignment with existing MinIO ops.

**Choose SeaweedFS** when object count and storage efficiency dominate (many small objects, filer-oriented layout), and you can operate the extra components.

**Choose AWS S3 / GCS / Azure** in production unless policy forces on-prem — then run this lab's comparisons as a **smoke test**, not a benchmark cert.

### Link to Experiment 09

Every Iceberg commit adds metadata files and often new data files. A streaming pipeline on **either** backend will create:

- Many small Parquet files → read amplification
- Many manifest entries → planning slowdown
- Many objects under `metadata/` → LIST amplification

Compaction (exp 09) fixes file size; **storage choice** affects how painful LIST and ops become at billions of objects.

---

## 🇪🇺 Regulatory angle (optional)

**DORA / portability:** Iceberg's open table spec plus swappable S3-compatible storage supports the story that data is not locked to one proprietary stack. In interviews, pair **open format (Iceberg)** with **replaceable object store** — not “we picked one vendor appliance forever.”

---

## ✍️ Re-answer the interview question

> "How would you choose MinIO vs SeaweedFS for Iceberg? What does Iceberg require from storage?"

Your answer should cover:

1. Iceberg needs standard S3 APIs + acceptable LIST/commit performance for your snapshot and file-size profile.
2. Both MinIO and SeaweedFS can host the same metadata tree; validate with real commits and reads.
3. MinIO: simpler S3 mental model; SeaweedFS: scale/cost profile for huge object counts.
4. Neither removes small-file or GC problems — those are table-maintenance concerns.
5. Production default: cloud S3-class storage; on-prem choice is an ops and economics decision.

Aim for 90 seconds.

---

## 🎁 LinkedIn post draft

> **"We ran the same Iceberg table on MinIO and SeaweedFS side by side. The metadata tree was identical; the bill wasn't."**
>
> Short post: 200 single-row commits, object counts, and why LIST matters as much as Parquet size. Link to this repo's Experiment 13.

---

## Session wrap-up (close the loop)

1. Confirm hands-on is done: notebook `lab/notebooks/08_storage_backends.ipynb` finished through the metrics table; Break it 1–3 completed (or reset afterward).
2. Update `interview-faq.md` with your **after** answer to this experiment's interview question.
3. If Break it left the lab in a broken state: `cd lab && ./reset.sh --confirm` — see [operation guide §7](../docs/operation-guide.md).
4. **End of day** (pause until tomorrow, keep data): `cd lab && docker compose stop` — [operation guide §8](../docs/operation-guide.md).
5. **Done with the lab on this machine** (remove all local tables and catalogs): [operation guide §10](../docs/operation-guide.md).

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md). Full 90-minute checklist: [operation guide §5.3](../docs/operation-guide.md).

---

## Next up

→ [Experiment 12: Iceberg for Regulated Data](12-iceberg-for-regulated-data.md) — EU FinTech specialty (recommended after 11 → 13 → 12), or [Experiment 14: Row-level mutations](14-row-level-mutations.md) if you are continuing straight into the advanced production track.

If you skipped Experiment 12's prerequisites, finish the production fundamentals self-check in [`experiments/README.md`](README.md) first.
