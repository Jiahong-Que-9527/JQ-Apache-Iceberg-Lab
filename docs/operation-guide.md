# Operation Guide

This guide walks through the full workflow: get the repo, start the lab, run experiments, reset when needed, and shut everything down when you are done.

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
- MinIO console: `http://localhost:9001`
- MinIO login: `minioadmin` / `minioadmin`

Check containers:

```bash
docker compose ps
```

You should see `minio` healthy and `jupyter` running.

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

For each experiment:

1. Read the interview question at the top.
2. Write your current answer in a local `interview-faq.md`.
3. Run the hands-on notebook or commands.
4. Do the "Break it" section.
5. Rewrite your answer after the experiment.

Do not commit `interview-faq.md` unless you intentionally want to publish it.

## 6. Inspect MinIO and Catalog State

Open MinIO at `http://localhost:9001` and browse:

```text
warehouse/lab/events/
```

Useful paths:

```text
warehouse/lab/events/metadata/
warehouse/lab/events/data/
```

From the `lab/` directory, you can inspect the SQLite catalog:

```bash
sqlite3 catalog.db ".tables"
sqlite3 catalog.db "select * from iceberg_tables;"
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
lab/warehouse/
lab/catalog.db
lab/catalog.db-journal
```

Then it starts the lab again. After Docker images are built, this should take only a few seconds.

## 8. Stop the Lab

If you are done for the day and want to stop all running containers while keeping data:

```bash
cd lab
docker compose stop
```

Start again later:

```bash
cd lab
docker compose up -d minio
docker compose up bucket-init
docker compose up -d jupyter
```

The simpler option is also fine:

```bash
cd lab
./init.sh
```

## 9. Shut Down and Remove Containers

If you want to stop and remove the lab containers and network while keeping local data files:

```bash
cd lab
docker compose down --remove-orphans
```

This keeps:

```text
lab/warehouse/
lab/catalog.db
lab/.env
```

Run `./init.sh` later to bring the lab back.

## 10. Full Local Cleanup

If you want to remove containers and delete all lab runtime state:

```bash
cd lab
docker compose down --remove-orphans
rm -rf warehouse catalog.db catalog.db-journal
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
lab/warehouse/
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
