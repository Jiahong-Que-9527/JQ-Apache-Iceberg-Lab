# Iceberg Lab — Build Spec

**Audience:** Code agent (implementer)
**Author:** Jiahong Que
**Status:** Ready for execution
**Estimated implementation time:** 30–45 min

---

## 0. Context (do not skip — this constrains every decision)

This is a **personal learning sandbox**, not a production system. Two non-negotiable constraints:

1. **Start fast, reset fast.** Cold start < 60s, full reset < 10s. If any design choice violates this, pick the simpler alternative.
2. **Maximize Iceberg surface area, minimize everything else.** No Spark, no Hive Metastore, no Nessie, no Kubernetes. Every minute spent on infra is a minute not spent learning Iceberg.

The user already runs a production-style lakehouse (SLH) with Trino + Iceberg + MinIO. **Do not duplicate it.** This sandbox is for breaking things, inspecting metadata, and running edge-case experiments without touching SLH.

**Success criterion for this build:** the user can `docker compose up`, open a Jupyter notebook, and within 2 minutes have created an Iceberg table, written data to it, and inspected the resulting `metadata.json` file by hand.

---

## 1. Architecture

```
┌─────────────────────────────────────────────┐
│  Host machine (user's laptop)               │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │  Jupyter Lab  (Python 3.11)          │  │
│  │  ├── pyiceberg[s3fs,duckdb,sql-sqlite]│ │
│  │  ├── duckdb                          │  │
│  │  └── pyarrow, pandas                 │  │
│  └─────────────┬────────────────────────┘  │
│                │                             │
│  ┌─────────────┴──────────┐                │
│  │  catalog.db (SQLite)   │  ← single file │
│  └─────────────┬──────────┘                │
│                │                             │
│  ┌─────────────┴──────────┐                │
│  │  MinIO (Docker)        │  ← S3 emulator│
│  │  bucket: warehouse     │                │
│  └────────────────────────┘                │
└─────────────────────────────────────────────┘
```

**Why these choices:**

| Component | Choice | Rationale |
|---|---|---|
| Compute | PyIceberg + DuckDB | No JVM. Starts in seconds. Lets the user inspect raw metadata files directly. |
| Catalog | SQLite (via `pyiceberg.catalog.sql`) | Single file. Zero ops. User can `sqlite3 catalog.db` and see exactly what the catalog stores. |
| Storage | MinIO single container | Mirrors SLH's S3 abstraction. Familiar tooling. |
| Notebook | Jupyter Lab in Docker | Same image as MinIO compose. Consistent env across machines. |

**Explicitly rejected alternatives:**

- ❌ Spark + Hive Metastore → too heavy, JVM startup time, configuration tax
- ❌ Nessie catalog → adds a service for no learning gain over SQLite at this stage
- ❌ Trino → user already has Trino in SLH; sandbox shouldn't duplicate
- ❌ AWS S3 → introduces auth/cost complexity; MinIO is local and free

---

## 2. Directory Layout

```
iceberg-lab/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── reset.sh                       # nuke warehouse + catalog, restart MinIO
├── init.sh                        # first-time setup: create bucket, install deps
├── requirements.txt
├── catalog.db                     # SQLite catalog (gitignored)
├── warehouse/                     # MinIO data volume (gitignored)
├── notebooks/
│   ├── 00_setup_check.ipynb       # verify everything works
│   ├── 01_basics.ipynb            # create table, insert, read
│   ├── 02_metadata_anatomy.ipynb  # ← highest interview value
│   ├── 03_schema_evolution.ipynb
│   ├── 04_time_travel.ipynb
│   ├── 05_partitioning.ipynb
│   ├── 06_compaction.ipynb
│   └── 07_iceberg_vs_delta.ipynb  # comparison notes + small experiments
└── src/
    └── catalog_helper.py          # one-liner to get a configured catalog object
```

**Note on `catalog.db` and `warehouse/`:** must be in `.gitignore`. The user wants this repo public (GitHub portfolio piece), but data and catalog state should never be committed.

---

## 3. File Specifications

### 3.1 `docker-compose.yml`

Requirements:
- Two services: `minio` and `jupyter`
- MinIO exposes 9000 (S3 API) and 9001 (web console)
- Jupyter exposes 8888, mounts `./notebooks` and `./src` into the container
- Both services on the same network so Jupyter can reach MinIO at `http://minio:9000`
- MinIO credentials come from `.env` (use `.env.example` as template)
- Jupyter image: `quay.io/jupyter/minimal-notebook:python-3.11` or equivalent
- Auto-install `requirements.txt` on Jupyter startup
- Healthcheck on MinIO so Jupyter waits for it

**Important:** Jupyter must start with `--NotebookApp.token=''` and `--NotebookApp.password=''` for zero-friction local dev. This is acceptable because this is a localhost sandbox. **Add a comment in the file explicitly saying "DO NOT use this config in any networked environment."**

### 3.2 `.env.example`

```
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=warehouse
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://minio:9000
```

The duplication (MINIO_* and AWS_*) is intentional — MinIO uses the first set, PyIceberg uses the AWS-prefixed set. Document this in README.

### 3.3 `requirements.txt`

```
pyiceberg[s3fs,sql-sqlite,duckdb,pyarrow]>=0.7.0
duckdb>=1.0.0
pandas>=2.0.0
pyarrow>=15.0.0
boto3
jupyter
ipywidgets
```

Pin only the lower bounds. Let the agent install latest within those bounds.

### 3.4 `init.sh`

Should do, idempotently:
1. Copy `.env.example` → `.env` if `.env` doesn't exist
2. `docker compose up -d minio`
3. Wait for MinIO health
4. Create the `warehouse` bucket using `mc` (MinIO client) or boto3 (whichever the agent finds cleaner)
5. `docker compose up -d jupyter`
6. Print: "Open http://localhost:8888 and start with notebooks/00_setup_check.ipynb"

### 3.5 `reset.sh`

Must complete in < 10 seconds. Do:
1. `docker compose down` (keep volumes? no — wipe them)
2. `rm -rf warehouse/ catalog.db`
3. `bash init.sh`

Add a `--confirm` flag so it doesn't fire accidentally.

### 3.6 `src/catalog_helper.py`

A single function `get_catalog()` that returns a configured `SqlCatalog` pointing at `catalog.db` with S3 credentials from env. This is imported in every notebook so the user doesn't repeat 10 lines of config.

```python
# Pseudocode — agent should implement properly
def get_catalog(name: str = "lab") -> SqlCatalog:
    return SqlCatalog(
        name,
        uri="sqlite:///catalog.db",
        warehouse=f"s3://{os.environ['MINIO_BUCKET']}/",
        **{
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ["AWS_REGION"],
        },
    )
```

### 3.7 Notebooks — Detailed Specs

Each notebook must follow this structure:
1. **Cell 1 — markdown:** "What you'll learn" (3 bullets max) + "Interview questions this answers" (2–3 questions)
2. **Cell 2 — code:** import + `catalog = get_catalog()`
3. **Body:** 5–10 cells alternating code + markdown explanation
4. **Last cell — markdown:** "Try yourself" (2 experiments the user should run before moving on)

#### `00_setup_check.ipynb`
- Verify imports work
- List buckets, confirm `warehouse` exists
- Print pyiceberg version, duckdb version, pyarrow version
- Print: "✅ Setup OK — proceed to 01_basics"

#### `01_basics.ipynb`
- Create namespace `lab`
- Define schema with 4–5 columns including one nested struct
- Create table `lab.events`
- Write a small pyarrow table (50 rows)
- Read back with `table.scan().to_arrow()`
- Read back with DuckDB SQL (`SELECT * FROM iceberg_scan('s3://warehouse/lab/events')`)
- **Interview questions answered:** "What does an Iceberg table physically consist of?" "How do you read Iceberg without Spark?"

#### `02_metadata_anatomy.ipynb` — **Highest priority notebook**
After running `01_basics`, this notebook **opens the metadata files by hand** and explains every field. Specifically:
- Use boto3 or mc to download `s3://warehouse/lab/events/metadata/v1.metadata.json` to local
- Pretty-print it. Walk through every top-level key: `format-version`, `table-uuid`, `location`, `last-sequence-number`, `schemas`, `current-schema-id`, `partition-specs`, `snapshots`, `refs`, `properties`
- Download the manifest list (the `.avro` file referenced in `current-snapshot-id`). Use `pyarrow.parquet` or the `fastavro` library to read it. Show: each entry points to a manifest file, has `added_files_count`, `existing_files_count`, `deleted_files_count`, partition summary, etc.
- Download one manifest file. Show: each entry is a `data_file` record with `file_path`, `record_count`, `file_size_in_bytes`, column-level stats (`lower_bounds`, `upper_bounds`, `null_value_counts`).
- **Add a markdown diagram** showing: `metadata.json → manifest list → manifest → data files`
- **Interview questions answered:** "Walk me through what happens when you query an Iceberg table." "How does Iceberg prune files at query time?" "What's the difference between a manifest list and a manifest?"

This notebook alone is more valuable than the other six combined for interviews. **Implement it carefully.**

#### `03_schema_evolution.ipynb`
- Add a column → show new metadata version, old data still readable
- Rename a column → show `field-id` is preserved (this is the magic)
- Drop a column → show data files unchanged
- Read old snapshot with new schema → demonstrate ID-based resolution
- **Interview questions answered:** "Why is Iceberg schema evolution safe but Parquet's isn't?" "What's a field-id?"

#### `04_time_travel.ipynb`
- Insert 3 batches of data, each in a separate commit
- List snapshots via `table.snapshots()`
- Read as-of snapshot N: `table.scan(snapshot_id=...)`
- Rollback: `table.manage_snapshots().rollback_to_snapshot(...).commit()`
- Inspect `metadata.json` after rollback — note that history is **preserved**, not deleted
- **Interview questions answered:** "How does Iceberg implement ACID?" "What happens to old snapshots — are they deleted?"

#### `05_partitioning.ipynb`
- Create a table partitioned by `days(event_ts)` — show this is **hidden partitioning**
- Insert data spanning 3 days, observe directory structure in MinIO
- Run a query with a timestamp filter, show partition pruning happens automatically (no `WHERE event_date = ...` needed)
- Evolve partition spec: change from `days` to `hours` — show old data keeps old partitioning, new data uses new
- **Interview questions answered:** "What's hidden partitioning and why does it matter?" "Can you change partitioning without rewriting data?"

#### `06_compaction.ipynb`
- Write 20 tiny batches (1 row each) → 20 small files
- Show file count via `table.inspect.files()`
- Run compaction via PyIceberg's `rewrite_data_files` action (if available in current version; otherwise use DuckDB to read-and-rewrite)
- Show file count after
- Run `expire_snapshots` to remove old metadata
- **Interview questions answered:** "Small file problem — how do you handle it?" "What are the risks of expiring snapshots?"

#### `07_iceberg_vs_delta.ipynb`
- Mostly markdown, not code
- A comparison table: transaction protocol, schema evolution, partitioning, catalog model, ecosystem
- 1–2 code cells showing the **same operation** in pseudo-Delta vs Iceberg
- **End with: "If asked in interview 'why Iceberg over Delta for SLH', here's the 90-second answer:"** (leave a markdown cell template for the user to fill in — this forces them to articulate the answer themselves)

### 3.8 `README.md`

Sections:
1. **What this is** (3 sentences, including link to SLH for context)
2. **Quick start** (`./init.sh`, open localhost:8888, start with `00_setup_check`)
3. **Reset** (when and how)
4. **Architecture diagram** (the ASCII one from §1)
5. **Why these choices** (the rationale table from §1 — this is what makes the repo a portfolio piece)
6. **What this is NOT** (explicitly: not a production system, not a SLH replacement, not networked-safe)
7. **Further reading** (links to Iceberg spec, key blog posts)

The README is **part of the deliverable**. It's what a hiring manager will read first when looking at this repo on GitHub. Write it as if explaining the design to a senior engineer who will judge you on it.

### 3.9 `.gitignore`

```
catalog.db
catalog.db-journal
warehouse/
.env
.ipynb_checkpoints/
__pycache__/
*.pyc
.DS_Store
```

---

## 4. Implementation Order (for the agent)

Do **not** build the notebooks first. Order matters:

1. `docker-compose.yml` + `.env.example` + `.gitignore` + `requirements.txt`
2. `init.sh` + verify MinIO starts, bucket creates, Jupyter is reachable
3. `src/catalog_helper.py`
4. `notebooks/00_setup_check.ipynb` — **stop here, ask user to verify before continuing**
5. `notebooks/01_basics.ipynb`
6. `notebooks/02_metadata_anatomy.ipynb` (the critical one)
7. Remaining notebooks 03–07
8. `reset.sh`
9. `README.md` (last — it describes what was actually built)

**Checkpoint after step 4.** If the user can't get `00_setup_check` to run green, no other notebook will work. Don't waste time building on a broken foundation.

---

## 5. Acceptance Criteria

Jiahong should be able to:

- [ ] Clone the repo on a fresh machine and run `./init.sh` successfully
- [ ] Open `00_setup_check.ipynb` and have every cell execute green
- [ ] Run `01_basics` end-to-end and see Iceberg files appear in MinIO console
- [ ] Run `02_metadata_anatomy` and **see the actual JSON contents** of a metadata file printed in the notebook
- [ ] Run `./reset.sh` and have a clean state in < 10 seconds
- [ ] Push the repo to GitHub and have the README render as a legible portfolio piece
- [ ] Use this environment to answer the 15+ interview questions listed across all notebooks **without looking anything up**

---

## 6. What the Agent Should NOT Do

- ❌ Add Trino, Spark, Nessie, or any other compute/catalog engine "for completeness"
- ❌ Add Kubernetes, helm charts, or any orchestration beyond docker-compose
- ❌ Add CI/CD, pre-commit hooks, or testing infrastructure (this is a sandbox, not a product)
- ❌ Write 500-line notebooks. Each notebook should be **readable in 10 minutes**.
- ❌ Skip the markdown explanations in notebooks. The explanations ARE the learning artifact.
- ❌ Use `latest` Docker tags. Pin to a specific version for reproducibility.

---

## 7. Open Questions (agent should ask before implementing)

1. Is the user on Linux, macOS (Intel), or macOS (Apple Silicon)? Affects Docker image arch tags.
2. Does the user want to publish this repo public on day 1, or build first and publish after review?
3. Any preference for notebook style (e.g., `nbqa` formatting, max line length)?

---

## 8. Deliverable Format

When done, the agent should:
1. Print a summary of files created
2. Print the exact commands the user runs next (`cd iceberg-lab && ./init.sh`)
3. Print 3 interview-style questions the user should attempt to answer **before** opening notebook 02 — to anchor learning to outcomes, not consumption

---

**End of spec.**
