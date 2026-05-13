# Iceberg Lab - Build Spec

**Audience:** Code agent (implementer)
**Author:** Jiahong Que
**Status:** Ready for execution
**Estimated implementation time:** 30-45 min for runtime, longer if all notebooks are expanded deeply

---

## 0. Context

This is a **personal learning sandbox**, not a production system. Two constraints drive every implementation choice:

1. **Start fast, reset fast.** Warm start should be under 60 seconds, reset should be under 10 seconds after images are already built.
2. **Maximize Iceberg surface area, minimize everything else.** No Spark, Hive Metastore, Nessie, Kubernetes, or Trino. The point is to inspect Iceberg metadata and failure modes, not to operate a production stack.

The user already has a production-style lakehouse with Trino, Iceberg, and MinIO. This lab must stay separate and disposable.

Success criterion: from a fresh clone, the user can run `cd lab && ./init.sh`, open JupyterLab, create an Iceberg table, write data, and inspect the resulting metadata files by hand.

---

## 1. Architecture

```
Repository root
└── lab/
    ├── JupyterLab container (Python 3.11)
    │   ├── PyIceberg
    │   ├── DuckDB
    │   ├── PyArrow / pandas
    │   └── fastavro for Iceberg manifest inspection
    ├── catalog.db      # SQLite Iceberg catalog, gitignored
    └── warehouse/      # MinIO object data, gitignored

MinIO container
└── bucket: warehouse
```

**Why these choices:**

| Component | Choice | Rationale |
| --- | --- | --- |
| Compute | PyIceberg + DuckDB | No JVM. Starts quickly. Lets the user inspect raw Iceberg metadata directly. |
| Catalog | SQLite via PyIceberg `SqlCatalog` | Single file, zero ops, visible with `sqlite3 catalog.db`. |
| Storage | MinIO single container | Local S3-compatible object store that mirrors the user's production abstraction. |
| Notebook | JupyterLab in Docker | Reproducible Python environment across machines. |
| Manifest reader | `fastavro` | Iceberg manifest lists and manifests are Avro files, not Parquet files. |

Explicitly rejected: Spark, Hive Metastore, Nessie, Kubernetes, Trino, AWS S3.

---

## 2. Directory Layout

All runnable lab files live under `lab/`. The repository root remains the portfolio/documentation entrypoint.

```
JQ-Apache-Iceberg-Lab/
├── README.md
├── docs/
│   ├── contributor-policy.md
│   └── iceberg-lab-spec.md
├── experiments/
│   └── *.md
└── lab/
    ├── Dockerfile
    ├── docker-compose.yml
    ├── .env.example
    ├── init.sh
    ├── reset.sh
    ├── requirements.txt
    ├── catalog.db                 # gitignored runtime state
    ├── warehouse/                 # gitignored runtime state
    ├── notebooks/
    │   ├── 00_setup_check.ipynb
    │   ├── 01_basics.ipynb
    │   ├── 02_metadata_anatomy.ipynb
    │   ├── 03_schema_evolution.ipynb
    │   ├── 04_time_travel.ipynb
    │   ├── 05_partitioning.ipynb
    │   ├── 06_compaction.ipynb
    │   └── 07_iceberg_vs_delta.ipynb
    └── src/
        └── catalog_helper.py
```

Runtime state must never be committed: `lab/catalog.db`, `lab/catalog.db-journal`, `lab/warehouse/`, `lab/.env`, notebook checkpoints, caches, and local interview notes.

---

## 3. Runtime Files

### 3.1 `lab/Dockerfile`

Use a pinned, Python 3.11 Jupyter Docker Stacks image:

```dockerfile
FROM quay.io/jupyter/minimal-notebook:python-3.11
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

Do not install Python dependencies on every Jupyter startup. Building the image once keeps warm starts fast.

### 3.2 `lab/docker-compose.yml`

Requirements:

- Services: `minio`, `bucket-init`, and `jupyter`.
- MinIO exposes S3 API `9000` and console `9001`.
- Jupyter exposes `8888`.
- All host port bindings must be localhost-only, for example `127.0.0.1:8888:8888`.
- Jupyter token and password are disabled for zero-friction local use. Add an explicit warning comment: **DO NOT use this config in any networked environment.**
- `bucket-init` uses a pinned MinIO Client image to create the `warehouse` bucket idempotently.
- Jupyter mounts the entire `lab/` directory at `/home/jovyan/work` so notebooks, `src/`, `catalog.db`, and `warehouse/` stay together.

### 3.3 `lab/.env.example`

```
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=warehouse
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://minio:9000
CATALOG_URI=sqlite:///catalog.db
```

The `MINIO_*` values configure MinIO. The AWS-prefixed values configure PyIceberg, boto3, and DuckDB S3 access.

### 3.4 `lab/requirements.txt`

```
pyiceberg[s3fs,sql-sqlite,duckdb,pyarrow]==0.11.0
duckdb==1.5.2
pandas==3.0.3
pyarrow==24.0.0
boto3==1.43.0
botocore==1.43.0
s3fs==2026.4.0
fsspec==2026.4.0
aiobotocore==3.7.0
fastavro==1.12.2
ipywidgets==8.1.8
```

These versions are pinned because `boto3`, `s3fs`, `aiobotocore`, and `botocore` otherwise trigger long dependency backtracking during image builds. Upgrade them as a tested set.

### 3.5 `lab/init.sh`

Idempotent startup:

1. Copy `.env.example` to `.env` if needed.
2. Create `warehouse/` if needed.
3. Ensure `lab/`, `warehouse/`, `notebooks/`, and `src/` are writable by the Jupyter container user.
4. Build the Jupyter image.
5. Start MinIO.
6. Run `bucket-init` to create the bucket.
7. Start Jupyter.
8. Print:

```text
Open http://localhost:8888 and start with notebooks/00_setup_check.ipynb
MinIO console: http://localhost:9001
```

### 3.6 `lab/reset.sh`

Require `--confirm`.

1. `docker compose down --remove-orphans`
2. `rm -rf warehouse catalog.db catalog.db-journal`
3. `bash init.sh`

This should be under 10 seconds after images are already built.

### 3.7 `lab/src/catalog_helper.py`

Provide:

- `get_catalog(name: str = "lab") -> SqlCatalog`
- `ensure_namespace(catalog, namespace: str = "lab")`
- `drop_table_if_exists(catalog, identifier: str)`
- `get_s3_client()`
- `current_metadata_location(table) -> str`
- `configure_duckdb_for_minio(con)`

Implementation notes:

- Use `SqlCatalog.create_tables()` so the SQLite catalog tables are initialized automatically.
- Use `s3.force-virtual-addressing = false` for MinIO path-style access.
- Keep `CATALOG_URI=sqlite:///catalog.db` relative to `/home/jovyan/work`.
- DuckDB S3 reads should use the metadata JSON path, not only the table root path.

---

## 4. Notebook Requirements

Each notebook follows the same structure:

1. Markdown: "What you'll learn" and "Interview questions this answers".
2. Code: imports and `catalog = get_catalog()`.
3. 5-10 cells alternating code and explanation.
4. Final markdown: "Try yourself".

### `00_setup_check.ipynb`

- Verify imports.
- List S3 buckets and confirm `warehouse` exists.
- Initialize the SQLite catalog.
- Print PyIceberg, DuckDB, PyArrow, pandas, and boto3 versions.
- Print `Setup OK - proceed to 01_basics`.

### `01_basics.ipynb`

- Create namespace `lab`.
- Create `lab.events` with 4-5 fields and one nested struct.
- Append about 50 rows.
- Read back with PyIceberg.
- Read back with DuckDB by passing `current_metadata_location(table)` to `iceberg_scan(...)`.

### `02_metadata_anatomy.ipynb`

After `01_basics`, inspect the real metadata tree:

- Load `lab.events`.
- Read the **current** metadata JSON from `current_metadata_location(table)`. Do not hard-code `v1.metadata.json`.
- Walk top-level keys: `format-version`, `table-uuid`, `location`, `last-sequence-number`, `schemas`, `current-schema-id`, `partition-specs`, `snapshots`, `refs`, and `properties`.
- Read the current snapshot's manifest list Avro with `fastavro`.
- Read one manifest Avro with `fastavro`.
- Show the pointer chain: `metadata.json -> manifest list -> manifest -> data files`.

### `03_schema_evolution.ipynb`

Demonstrate add, rename, and drop at the concept/API level. If an operation is version-sensitive in PyIceberg, include a small guarded code path and explain the expected metadata effect.

### `04_time_travel.ipynb`

Append several batches, list snapshots, read an older snapshot, and explain rollback/history preservation.

### `05_partitioning.ipynb`

Create a hidden-partitioned table, write data spanning days, and show how the partition spec appears in metadata.

### `06_compaction.ipynb`

Write many tiny files, inspect file metadata, and explain compaction. If PyIceberg's maintenance API is unavailable in the installed version, leave a guarded fallback explanation rather than faking compaction.

### `07_iceberg_vs_delta.ipynb`

Mostly markdown. Compare protocol, schema evolution, partitioning, catalog model, and ecosystem. End with a fill-in 90-second interview answer template.

---

## 5. README Requirements

The root `README.md` should keep the portfolio framing and add a runnable lab quick start:

```bash
cd lab
./init.sh
```

It must document:

- Jupyter: `http://localhost:8888`
- MinIO console: `http://localhost:9001`
- Reset: `cd lab && ./reset.sh --confirm`
- Localhost-only security boundary
- The fact that runtime state is gitignored

---

## 6. Acceptance Criteria

- [ ] `cd lab && ./init.sh` succeeds on this machine.
- [ ] `docker compose ps` shows MinIO healthy and Jupyter running.
- [ ] `00_setup_check.ipynb` can execute green.
- [ ] `01_basics.ipynb` can create an Iceberg table and append data.
- [ ] `02_metadata_anatomy.ipynb` can show the actual metadata JSON and Avro manifest chain.
- [ ] `./reset.sh --confirm` recreates a clean lab.
- [ ] `git shortlog -sne --all` shows only `Jiahong Que <jiahongque25@gmail.com>`.

---

## 7. Code Agent Rules

- Follow `../AGENTS.md`.
- Do not add another Git contributor.
- Do not add production infrastructure.
- Do not commit runtime state.
- Do not use `latest` Docker tags.
- If a PyIceberg API is version-sensitive, guard the notebook cell and explain the expected behavior instead of writing misleading code.

---

**End of spec.**
