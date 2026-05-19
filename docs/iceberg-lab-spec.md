# Iceberg Lab - Build Spec

**Audience:** Code agent (implementer)
**Author:** Jiahong Que
**Status:** Ready for execution
**Estimated implementation time:** 30-45 min for runtime, longer if all notebooks are expanded deeply

---

## 0. Context

This is a **personal learning sandbox**, not a production system. Two constraints drive every implementation choice:

1. **Start fast, reset fast.** Warm start should be under 60 seconds, reset should be under 10 seconds after images are already built.
2. **Maximize Iceberg surface area, minimize everything else.** The default lab is PyIceberg + DuckDB + object storage only. Spark, Trino, and Lakekeeper live in a separate opt-in profile used only where an experiment explicitly needs JVM SQL procedures, a REST catalog server, or multi-engine validation.

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
    ├── catalog.db          # SQLite Iceberg catalog, gitignored
    ├── warehouse-minio/    # MinIO object data, gitignored
    ├── warehouse-seaweed/  # SeaweedFS object data, gitignored
    └── spark-profile/      # optional Spark + Trino + Lakekeeper stack

MinIO container ──► bucket: warehouse
SeaweedFS container (S3 :8333) ──► bucket: warehouse
Spark profile ─────► joins main lab network, uses MinIO, keeps Lakekeeper state in spark-profile/state/
```

**Why these choices:**

| Component | Choice | Rationale |
| --- | --- | --- |
| Compute | PyIceberg + DuckDB by default; Spark only in `lab/spark-profile/` | Main path starts quickly and keeps the focus on metadata. Spark is reserved for experiments 14, 16, 17, 20, 21. |
| Catalog | SQLite via PyIceberg `SqlCatalog` | Single file, zero ops, visible with `sqlite3 catalog.db`. |
| Storage | MinIO + SeaweedFS (parallel) | MinIO is the default for experiments 01–12. SeaweedFS is added for Experiment 13 only — S3 semantics and Iceberg ops comparison without Spark or extra catalogs. |
| Notebook | JupyterLab in Docker | Reproducible Python environment across machines. |
| Manifest reader | `fastavro` | Iceberg manifest lists and manifests are Avro files, not Parquet files. |
| REST catalog profile | Lakekeeper + Postgres | Production-catalog learning path for experiment 16 and shared catalog for Spark/Trino interop. |
| JVM engines | Spark + Trino | Required for MERGE, branch-aware writes, migration procedures, and cross-engine compatibility experiments. |

Still rejected for the default lab: Hive Metastore, Nessie, Kubernetes, and AWS S3. Spark and Trino are allowed only through the documented opt-in profile.

### Why SeaweedFS is in the lab

Experiment 13 compares two S3-compatible on-prem options. SeaweedFS is **not** required for Iceberg fundamentals (01–12). It exists so learners can measure the same commit/LIST workload on two backends and defend storage choices in interviews.

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
    │   ├── 07_iceberg_vs_delta.ipynb
    │   └── 08_storage_backends.ipynb
    ├── seaweedfs/
    │   └── s3-config.json
    ├── spark-profile/
    │   ├── docker-compose.yml
    │   ├── up.sh
    │   ├── down.sh
    │   ├── spark/
    │   ├── trino/
    │   └── state/                  # gitignored Lakekeeper/Postgres runtime state
    ├── scripts/
    │   └── ensure_bucket.py
    └── src/
        └── catalog_helper.py
```

Runtime state must never be committed: `lab/catalog.db`, `lab/catalog.db-journal`, `lab/catalog_seaweed.db`, `lab/catalog_seaweed.db-journal`, `lab/warehouse/`, `lab/warehouse-minio/`, `lab/warehouse-seaweed/`, `lab/spark-profile/state/`, `lab/.env`, notebook checkpoints, caches, and local interview notes.

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

- Services: `minio`, `bucket-init`, `seaweedfs`, `seaweed-bucket-init`, and `jupyter`.
- MinIO exposes S3 API `9000` and console `9001`; data volume `warehouse-minio/`.
- SeaweedFS exposes S3 API `8333` and master UI `9333`; data volume `warehouse-seaweed/`; S3 config in `seaweedfs/s3-config.json`.
- Jupyter exposes `8888`.
- All host port bindings must be localhost-only, for example `127.0.0.1:<host-port>:<container-port>`.
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
S3_ENDPOINT_MINIO=http://minio:9000
CATALOG_URI=sqlite:///catalog.db
S3_ENDPOINT_SEAWEED=http://seaweedfs:8333
SEAWEED_BUCKET=warehouse
SEAWEED_ACCESS_KEY_ID=seaweedadmin
SEAWEED_SECRET_ACCESS_KEY=seaweedadmin
CATALOG_URI_SEAWEED=sqlite:///catalog_seaweed.db
```

The `MINIO_*` values configure MinIO. The AWS-prefixed values configure PyIceberg/boto3/DuckDB for **MinIO** (default). Seaweed-prefixed values configure the second backend for Experiment 13.

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
2. Create `warehouse-minio/` and `warehouse-seaweed/` if needed.
3. Ensure lab dirs are writable by the Jupyter container user.
4. Build the Jupyter image.
5. Start MinIO and SeaweedFS.
6. Run `bucket-init` and `seaweed-bucket-init`.
7. Start Jupyter.
8. Print Jupyter, MinIO console, and SeaweedFS UI URLs.

### 3.6 `lab/reset.sh`

Require `--confirm`.

1. `docker compose down --remove-orphans`
2. `rm -rf warehouse-minio warehouse-seaweed warehouse catalog.db catalog.db-journal catalog_seaweed.db catalog_seaweed.db-journal`
3. `bash init.sh`

This should be under 10 seconds after images are already built.

### 3.7 `lab/src/catalog_helper.py`

Provide:

- `get_catalog(backend: Literal["minio","seaweed"] = "minio", name: str | None = None) -> SqlCatalog`
- `ensure_namespace`, `drop_table_if_exists`
- `get_s3_client(backend=...)`, `list_object_keys(prefix, backend=...)`
- `download_s3_uri`, `read_json_s3` with optional `backend`
- `current_metadata_location(table) -> str`
- `configure_duckdb_for_s3(con, backend=...)`, alias `configure_duckdb_for_minio`
- `storage_console_url(backend)`

Implementation notes:

- MinIO uses `catalog.db`; Seaweed uses `catalog_seaweed.db`.
- Default `get_catalog()` remains MinIO so experiments 01–12, 15, 18, and 19 stay on the lightweight path.
- Use `s3.force-virtual-addressing = false` for path-style access on both backends.

### 3.8 `lab/spark-profile/`

Opt-in profile for experiments 14, 16, 17, 20, and 21.

Requirements:

- Requires the main lab network (`jq-apache-iceberg-lab_default`) to exist; users must run `cd lab && ./init.sh` first.
- Services: `lakekeeper-db`, `lakekeeper-migrate`, `lakekeeper`, `lakekeeper-bootstrap`, `spark-iceberg`, and `trino`.
- Lakekeeper image must be pinned by tag or digest, not `latest`.
- Spark image: `tabulario/spark-iceberg:3.5.5_1.7.1`.
- Trino image: `trinodb/trino:455`.
- Lakekeeper UI/API binds to `127.0.0.1:8181`.
- Trino binds to `127.0.0.1:8090`.
- Spark's optional notebook port binds to `127.0.0.1:8889`, not `8888`, because the main lab's JupyterLab owns `8888`.
- Postgres state lives in `lab/spark-profile/state/` and is gitignored.
- `up.sh` should fail clearly if the main lab network is missing.
- `down.sh` should stop the profile while preserving `state/`.

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

### `08_storage_backends.ipynb`

Experiment 13 companion: health-check MinIO and SeaweedFS, mirror Iceberg tables on both, compare object counts, run a 200-commit small-file storm, output a metrics DataFrame.

### Spark-profile notebooks

Spark-profile notebooks are optional companions under `lab/spark-profile/spark/notebooks/`. They should only cover experiments that already require the profile. Currently:

- `14_row_level_mutations.ipynb` for experiment 14.

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
- Spark profile, when needed: `cd lab/spark-profile && ./up.sh`
- Reset: `cd lab && ./reset.sh --confirm`
- Localhost-only security boundary
- The fact that runtime state is gitignored

---

## 6. Acceptance Criteria

- [ ] `cd lab && ./init.sh` succeeds on this machine.
- [ ] `docker compose ps` shows MinIO and SeaweedFS healthy and Jupyter running.
- [ ] `08_storage_backends.ipynb` runs on a clean lab (Experiment 13).
- [ ] `00_setup_check.ipynb` can execute green.
- [ ] `01_basics.ipynb` can create an Iceberg table and append data.
- [ ] `02_metadata_anatomy.ipynb` can show the actual metadata JSON and Avro manifest chain.
- [ ] `cd lab/spark-profile && ./up.sh` starts Lakekeeper on `8181`, Trino on `8090`, and Spark without conflicting with Jupyter on `8888`.
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
