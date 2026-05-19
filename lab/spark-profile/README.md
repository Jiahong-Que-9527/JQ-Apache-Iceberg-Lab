# Spark Profile — Opt-in JVM Stack for Tier 3 Experiments

This profile is **not** part of the main lab. It is only needed for experiments **14, 16, 17, 20, 21**, where you need Lakekeeper, Trino, or APIs that PyIceberg cannot yet provide:

| Experiment | Why it needs Spark / Trino / Lakekeeper |
| --- | --- |
| 14 — Row-level mutations | `MERGE INTO`, `CALL system.rewrite_data_files` (CoW vs MoR control) |
| 16 — Production catalog | Lakekeeper REST catalog, plus Spark and Trino clients against the same catalog |
| 17 — Write-Audit-Publish | branch-aware writes (`WRITE … TO BRANCH`), `MERGE` on a branch |
| 20 — Multi-engine interop | Spark writes V2 delete files; Trino reads them; you observe compatibility surface |
| 21 — Hive → Iceberg migration | `add_files`, `snapshot`, in-place migration procedures |

The main lab keeps its 60-second cold start. This profile adds ~3 GB of images and ~3 minutes to first start. Don't enable it until you actually need it.

---

## Prerequisites

- The main lab must already be running (`cd lab && ./init.sh`) — this profile **joins the main lab's network** to share MinIO.
- About 6 GB of free RAM (Spark + Trino + Lakekeeper + Postgres on top of MinIO + Jupyter).

## Start

```bash
cd lab/spark-profile
./up.sh
```

That brings up:

- **Lakekeeper** REST catalog on `http://localhost:8181` (UI + management API)
- **Spark 3.5.5** with the Iceberg 1.7.1 runtime, talking to Lakekeeper
- **Trino 455** with an Iceberg catalog pointed at Lakekeeper, on `http://localhost:8090`
- **Postgres 16** holding Lakekeeper's metadata

Spark's optional notebook port is exposed on `http://localhost:8889`, so it does not collide with the main lab's JupyterLab on `http://localhost:8888`.

On first start it also bootstraps Lakekeeper and creates a warehouse named `lab` backed by the main lab's MinIO.

Verify:

```bash
curl -s http://localhost:8181/health
curl -s http://localhost:8090/v1/info
```

## Use Spark

```bash
docker compose exec spark-iceberg spark-sql
```

Inside the prompt:

```sql
USE rest_lab;
SHOW NAMESPACES;
CREATE NAMESPACE IF NOT EXISTS demo;
CREATE TABLE demo.t (id BIGINT, v STRING) USING iceberg;
INSERT INTO demo.t VALUES (1, 'a'), (2, 'b');
SELECT * FROM demo.t;
```

For a notebook-style workflow:

```bash
docker compose exec spark-iceberg pyspark
```

The Spark client is preconfigured (`spark-defaults.conf`) with the `rest_lab` catalog pointing at Lakekeeper and the MinIO S3 endpoint, so the only thing you ever set in your code is the catalog name.

## Use Trino

```bash
docker compose exec trino trino
```

```sql
SHOW CATALOGS;             -- expect: iceberg, system, jmx, ...
SHOW SCHEMAS FROM iceberg; -- expect: demo, information_schema
SELECT * FROM iceberg.demo.t;
```

Trino is configured (`trino/catalog/iceberg.properties`) to use the same Lakekeeper REST catalog and MinIO bucket as Spark. Anything Spark writes, Trino reads, immediately.

## Stop

```bash
./down.sh
```

State (Postgres volume, written data files) is preserved on disk. Re-run `./up.sh` to come back.

## Full cleanup

```bash
./down.sh
rm -rf state/
```

This removes the Lakekeeper Postgres volume. Iceberg data files in MinIO remain — clean them via the main lab's `./reset.sh --confirm` if needed.

## Troubleshooting

- **`network jq-apache-iceberg-lab_default not found`** — the main lab is not running. Start it first with `cd lab && ./init.sh`.
- **Lakekeeper unhealthy** — check Postgres logs: `docker compose logs lakekeeper-db`.
- **Spark cannot reach MinIO** — confirm the main lab `minio` container is healthy and the bucket-init job completed.
- **Trino "Failed to query schemas"** — Lakekeeper hadn't finished bootstrapping when Trino started. `docker compose restart trino`.

## File map

```
spark-profile/
├── README.md
├── docker-compose.yml
├── up.sh
├── down.sh
├── spark/
│   ├── spark-defaults.conf
│   └── notebooks/              (mounted into the Spark container)
├── trino/
│   └── catalog/
│       └── iceberg.properties
└── state/
    └── lakekeeper-pg/          (Postgres data, gitignored)
```
