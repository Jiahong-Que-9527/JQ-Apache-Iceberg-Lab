# Operation Guide

This guide walks through the full workflow: get the repo, start the lab, run experiments, reset when needed, and shut everything down when you are done.

**If you only read one doc for day-to-day use, read this one.** The root [`README.md`](../README.md) is the portfolio overview; each file under [`experiments/`](../experiments/) is a single lab session.

If you are deciding which document to open first, use [`docs/how-to-use-this-project.md`](how-to-use-this-project.md).

---

## 0. Complete lifecycle (start → experiment → close)

Use this map to pick the right section. Every phase has concrete commands below.

| Phase | Goal | What you run | Section |
| --- | --- | --- | --- |
| **A. First-time setup** | Clone repo, install Docker, start lab | `git clone` → `cd lab` → `./init.sh` | §1–3 |
| **B. Verify** | Confirm containers and first notebooks | `docker compose ps` → `00_setup_check` → `01_basics` | §3–4 |
| **C. One experiment session** | Interview Q → hands-on → Break it → re-answer | `experiments/NN-*.md` + matching notebook | §5 |
| **D. Mid-session reset** | Clean tables after Break it without reinstalling Docker | `./reset.sh --confirm` | §7 |
| **E. End of day** | Stop containers, **keep** data for tomorrow | `docker compose stop` | §8 |
| **F. Done with the lab** | Remove containers and/or delete all local state | §9 or §10 | §9–10 |

```text
  [Clone] → ./init.sh → Jupyter + experiments (loop §5)
                ↑              │
                │         ./reset.sh --confirm (optional, §7)
                │              │
         docker compose stop (§8, keep data)
                │
         docker compose down (§9, keep data on disk)
                │
         rm warehouse* catalog*.db (§10, full wipe)
```

**Experiment 13:** needs **both** MinIO and SeaweedFS (started automatically by `./init.sh`). Follow §5.3 for the full 90-minute playbook.

**Experiments 14, 16, 17, 20, 21:** also need the opt-in Spark profile (Spark + Trino + Lakekeeper). Start the main lab first, then follow §5.4.

---

## 1. Prerequisites

Install these on your machine:

- Git
- Docker
- Docker Compose v2

Check them:

```bash
git --version
docker --version
docker compose version
```

The lab exposes services only on localhost:

- JupyterLab: `http://localhost:8888`
- MinIO console: `http://localhost:9001`
- SeaweedFS master UI: `http://localhost:9333` (S3 API: port `8333`)

Jupyter runs without a token/password for local learning. Do not expose this compose stack on a networked host.

## 2. Get the Repository

```bash
git clone https://github.com/Jiahong-Que-9527/JQ-Apache-Iceberg-Lab.git
cd JQ-Apache-Iceberg-Lab
```

The runnable environment lives in `lab/`. The long-form experiment writeups live in `experiments/`.

## 3. Start the Lab

```bash
cd lab
./init.sh
```

On the first run, Docker builds the Jupyter image and downloads dependencies. That can take a few minutes. Later starts are much faster because the image is cached.

When startup finishes, open:

- JupyterLab: `http://localhost:8888`
- MinIO console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- SeaweedFS master UI: `http://localhost:9333` (S3: `seaweedadmin` / `seaweedadmin`)

Check containers:

```bash
docker compose ps
```

You should see `minio` and `seaweedfs` healthy, `jupyter` running, and completed `bucket-init` / `seaweed-bucket-init` jobs.

Example (service names may vary slightly by Compose version):

```text
NAME          STATUS
minio         running (healthy)
seaweedfs     running (healthy)
jupyter       running
bucket-init   exited (0)
seaweed-bucket-init  exited (0)
```

If `jupyter` is not running, read the logs:

```bash
cd lab
docker compose logs jupyter --tail 50
docker compose logs seaweedfs --tail 30
```

Experiments **01–12, 15, 18, 19** use the main MinIO/PyIceberg lab (`get_catalog()` default). **Experiment 13** uses both backends (`get_catalog("minio")` and `get_catalog("seaweed")`). **Experiments 14, 16, 17, 20, 21** add the opt-in Spark profile for Lakekeeper, Spark SQL, Trino, and JVM-only Iceberg procedures.

## 4. Run the First Lab Notebooks

In JupyterLab, run these in order:

1. `notebooks/00_setup_check.ipynb`
2. `notebooks/01_basics.ipynb`
3. `notebooks/02_metadata_anatomy.ipynb`

The first notebook checks imports, MinIO access, and catalog initialization.

The second notebook creates `lab.events`, writes 50 rows, and reads them back with both PyIceberg and DuckDB.

The third notebook opens the Iceberg metadata chain:

```text
metadata.json -> manifest list -> manifest -> data files
```

This is the most important notebook for interview preparation because it shows how Iceberg actually finds data.

## 5. Use the Experiment Writeups

The Markdown experiments are under `experiments/`.

Recommended first pass:

1. `experiments/01-first-table.md`
2. `experiments/02-metadata-anatomy.md`
3. `experiments/04-reads-without-spark.md`
4. `experiments/05-snapshots-time-travel.md`
5. `experiments/07-hidden-partitioning.md`
6. `experiments/09-small-files-compaction.md`
7. `experiments/13-minio-vs-seaweedfs.md` with `notebooks/08_storage_backends.ipynb` (optional until Tier 3)

For each experiment:

1. Read the interview question at the top.
2. Write your current answer in a local `interview-faq.md`.
3. Run the hands-on notebook or commands.
4. Do the "Break it" section.
5. Rewrite your answer after the experiment.

Do not commit `interview-faq.md` unless you intentionally want to publish it.

### 5.1 Experiment ↔ notebook mapping

| Experiment | Markdown | Notebook (if any) | Storage |
| --- | --- | --- | --- |
| 01 | `01-first-table.md` | `01_basics.ipynb` | MinIO |
| 02 | `02-metadata-anatomy.md` | `02_metadata_anatomy.ipynb` | MinIO |
| 03 | `03-catalog-layer.md` | (Jupyter / ad hoc) | MinIO |
| 04 | `04-reads-without-spark.md` | (Jupyter / ad hoc) | MinIO |
| 05 | `05-snapshots-time-travel.md` | `04_time_travel.ipynb` | MinIO |
| 06 | `06-schema-evolution.md` | `03_schema_evolution.ipynb` | MinIO |
| 07 | `07-hidden-partitioning.md` | `05_partitioning.ipynb` | MinIO |
| 08 | `08-concurrent-writes.md` | (Jupyter / ad hoc) | MinIO |
| 09 | `09-small-files-compaction.md` | `06_compaction.ipynb` | MinIO |
| 10 | `10-expiration-gc.md` | (Jupyter / ad hoc) | MinIO |
| 11 | `11-iceberg-vs-delta-vs-hudi.md` | `07_iceberg_vs_delta.ipynb` | MinIO |
| 12 | `12-iceberg-for-regulated-data.md` | (Jupyter / ad hoc) | MinIO |
| 13 | `13-minio-vs-seaweedfs.md` | `08_storage_backends.ipynb` | **MinIO + SeaweedFS** |
| 14 | `14-row-level-mutations.md` | `spark-profile/spark/notebooks/14_row_level_mutations.ipynb` or `spark-sql` | **Spark profile + Lakekeeper + MinIO** |
| 15 | `15-cdc-incremental-reads.md` | (Jupyter / ad hoc) | MinIO |
| 16 | `16-production-catalog-rest.md` | `spark-sql`, `trino`, curl, PyIceberg REST | **Spark profile + Lakekeeper + MinIO** |
| 17 | `17-wap-branches-tags.md` | `spark-sql`, optional Trino | **Spark profile + Lakekeeper + MinIO** |
| 18 | `18-maintenance-observability.md` | (Jupyter / ad hoc) | MinIO; Spark profile optional for some `CALL system.*` procedures |
| 19 | `19-performance-tuning.md` | (Jupyter / ad hoc) | MinIO; Spark profile optional for some write/maintenance knobs |
| 20 | `20-multi-engine-interop.md` | `spark-sql`, `trino`, Jupyter | **Spark profile + Lakekeeper + MinIO** |
| 21 | `21-hive-to-iceberg-migration.md` | `spark-sql`, `trino` | **Spark profile + Lakekeeper + MinIO** |

### 5.2 After each experiment session (5-minute wrap-up)

1. Save `interview-faq.md` with your **after** answer.
2. If Break it left the lab broken, run `./reset.sh --confirm` (§7).
3. Optional: `docker compose stop` if you are done for the day (§8).
4. Note which experiment you will do next — no need to shut down Docker between evenings if you use §8.

### 5.3 Experiment 13 — full playbook (≈90 min)

Prerequisites: Tier 1 done; Experiment 09 recommended. Lab must be up (`./init.sh`).

| Step | Time | Action | Success check |
| --- | --- | --- | --- |
| 1 | 5 min | Open `experiments/13-minio-vs-seaweedfs.md`, write **before** answer in `interview-faq.md` | Question recorded |
| 2 | 5 min | In Jupyter, open `08_storage_backends.ipynb`, run **Step 1** (health check) | Both backends print `catalog OK` |
| 3 | 15 min | Run notebook **Steps 2–4** (mirror table, layout, reads) | 30 rows from PyIceberg and DuckDB on each backend |
| 4 | 20 min | Run notebook **Step 5** (200-commit storm) | DataFrame with `minio` and `seaweed` rows |
| 5 | 15 min | In MinIO console + Seaweed UI, browse `warehouse/lab/storage_compare/` | Same metadata/data layout on both |
| 6 | 25 min | Do **Break it** § in `13-minio-vs-seaweedfs.md` (at least Break 1–3) | You see expected failures |
| 7 | 5 min | Read Theory, write **after** answer in `interview-faq.md` | Delta vs your before answer |

If Step 2 fails on SeaweedFS:

```bash
cd lab
docker compose ps
docker compose logs seaweed-bucket-init
```

After Break it sections that say **Reset**, run:

```bash
cd lab
./reset.sh --confirm
```

Then re-run notebook Steps 2–4 only (skip the storm unless you are comparing again).

**When Experiment 13 is complete:** continue to Experiment 12 (EU track), Experiment 14 (advanced production track), or §8/§10 below to close the lab.

### 5.4 Spark profile playbook (experiments 14, 16, 17, 20, 21)

Start the main lab first:

```bash
cd lab
./init.sh
```

Then start the opt-in JVM stack from inside `lab/`:

```bash
cd spark-profile
./up.sh
```

Open or use:

- Main JupyterLab: `http://localhost:8888`
- Optional Spark notebook port: `http://localhost:8889`
- Lakekeeper: `http://localhost:8181`
- Trino: `http://localhost:8090`
- Spark SQL: `cd lab/spark-profile && docker compose exec spark-iceberg spark-sql`
- Trino CLI: `cd lab/spark-profile && docker compose exec trino trino`

The Spark profile joins the main lab Docker network and uses the main lab's MinIO bucket. It has its own Lakekeeper Postgres state under `lab/spark-profile/state/`.

When you finish a Spark-profile experiment:

```bash
cd lab/spark-profile
./down.sh
```

This stops Spark, Trino, Lakekeeper, and Postgres while keeping Lakekeeper state on disk. Use the main lab reset (§7) to wipe MinIO data and SQLite catalogs; use `cd lab/spark-profile && rm -rf state/` only when you also want to wipe Lakekeeper's catalog database.

## 6. Inspect Object Storage and Catalog State

### MinIO

Open MinIO at `http://localhost:9001` and browse:

```text
warehouse/lab/events/
```

Useful paths:

```text
warehouse/lab/events/metadata/
warehouse/lab/events/data/
```

### SeaweedFS

- Master/cluster UI: `http://localhost:9333`
- S3 API (from host): `http://localhost:8333`
- Browse the same logical paths under bucket `warehouse` (e.g. `lab/events/`)

### SQLite catalogs

MinIO tables use `catalog.db`. SeaweedFS tables use `catalog_seaweed.db`.

From the `lab/` directory:

```bash
sqlite3 catalog.db ".tables"
sqlite3 catalog.db "select * from iceberg_tables;"
sqlite3 catalog_seaweed.db "select * from iceberg_tables;"
```

If `sqlite3` is not installed on your host, inspect the catalog from a Jupyter notebook or terminal with Python:

```python
import sqlite3

con = sqlite3.connect("catalog.db")
con.execute("select * from iceberg_tables").fetchall()
```

## 7. Reset the Lab

Use reset when you want a clean warehouse and catalog:

```bash
cd lab
./reset.sh --confirm
```

This deletes:

```text
lab/warehouse-minio/
lab/warehouse-seaweed/
lab/catalog.db
lab/catalog.db-journal
lab/catalog_seaweed.db
lab/catalog_seaweed.db-journal
```

Then it starts the lab again. After Docker images are built, this should take only a few seconds.

## 8. Stop the Lab (end of day — keep data)

Use this when you finished a study session and will continue tomorrow. **Warehouse files and catalogs stay on disk.**

If the Spark profile is running, stop it first:

```bash
cd lab/spark-profile
./down.sh
```

If you are done for the day and want to stop all running containers while keeping data:

```bash
cd lab
docker compose stop
```

Start again later:

```bash
cd lab
docker compose up -d minio seaweedfs
docker compose up bucket-init seaweed-bucket-init
docker compose up -d jupyter
```

The simpler option is also fine:

```bash
cd lab
./init.sh
```

## 9. Shut Down and Remove Containers (keep data on disk)

Use this when you want Docker resources back (CPU/RAM) but may return to the same tables later. Same outcome as §8 for data files; containers are removed instead of only stopped.

If you want to stop and remove the lab containers and network while keeping local data files:

```bash
cd lab
docker compose down --remove-orphans
```

If the Spark profile is running, stop it first with `cd lab/spark-profile && ./down.sh`; it uses the main lab network.

This keeps:

```text
lab/warehouse-minio/
lab/warehouse-seaweed/
lab/catalog.db
lab/catalog_seaweed.db
lab/.env
```

Run `./init.sh` later to bring the lab back.

## 10. Full Local Cleanup (wipe everything)

Use this when you are **done with the project** on this machine, need disk space, or want a factory-fresh clone experience. This is irreversible for local tables and catalogs.

| Your situation | Command |
| --- | --- |
| Done for today, resume tomorrow | §8 `docker compose stop` |
| Free RAM, same data later | §9 `docker compose down` |
| Finished all experiments / starting over | §10 below |
| Break it corrupted state mid-experiment | §7 `./reset.sh --confirm` |

If you want to remove containers and delete all lab runtime state:

```bash
cd lab
docker compose down --remove-orphans
rm -rf warehouse-minio warehouse-seaweed warehouse catalog.db catalog.db-journal catalog_seaweed.db catalog_seaweed.db-journal
rm -rf spark-profile/state
```

Optional: remove the generated `.env` file too:

```bash
rm -f .env
```

Optional: remove the locally built Jupyter image:

```bash
docker image rm jq-apache-iceberg-lab-jupyter
```

## 11. What Should Not Be Committed

These are runtime files and should stay untracked:

```text
lab/.env
lab/catalog.db
lab/catalog.db-journal
lab/catalog_seaweed.db
lab/catalog_seaweed.db-journal
lab/warehouse/
lab/warehouse-minio/
lab/warehouse-seaweed/
lab/spark-profile/state/
lab/.ipynb_checkpoints/
lab/src/__pycache__/
```

Check before committing:

```bash
git status --short
```

## 12. Troubleshooting

If Jupyter cannot write `catalog.db`, rerun:

```bash
cd lab
./init.sh
```

The script makes the lab directory writable for the Jupyter container user.

If ports are already in use, check what is running:

```bash
docker ps
```

Then stop the old lab:

```bash
cd lab
docker compose down --remove-orphans
```

If notebooks fail after many experiments, reset to a known-clean state:

```bash
cd lab
./reset.sh --confirm
```

### Experiment 13 / dual-backend issues

- **Wrong backend for a table:** use `get_catalog("minio")` vs `get_catalog("seaweed")` to match where objects were written.
- **SeaweedFS not ready:** `docker compose ps` — wait for `seaweedfs` healthy before Jupyter starts.
- **Port 8333 or 9333 in use:** stop other stacks with `docker compose down --remove-orphans`.
- **RAM:** running MinIO + SeaweedFS + Jupyter needs roughly 4 GB free; close other Docker workloads if containers OOM.

### Spark profile issues

- **`network jq-apache-iceberg-lab_default not found`:** start the main lab first with `cd lab && ./init.sh`.
- **Port 8888 already in use:** expected if the main lab is running. Spark's optional notebook port is `http://localhost:8889`; main Jupyter stays on `http://localhost:8888`.
- **Lakekeeper unhealthy:** `cd lab/spark-profile && docker compose logs lakekeeper-db lakekeeper --tail 80`.
- **Trino cannot list schemas:** Lakekeeper may still be bootstrapping. Run `cd lab/spark-profile && docker compose restart trino`.
- **Need a full REST catalog reset:** `cd lab/spark-profile && ./down.sh && rm -rf state/ && ./up.sh`.
