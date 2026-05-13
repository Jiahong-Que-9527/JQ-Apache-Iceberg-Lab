# JQ Apache Iceberg Lab

Hands-on Apache Iceberg learning for data engineers who want interview-ready depth, not copy-paste tutorials. This repository pairs a lightweight local sandbox (PyIceberg, DuckDB, MinIO) with a twelve-part experiment series built around real interview questions, optional EU FinTech regulatory angles, and deliberate "break it" exercises.

## What is in this repo

| Path | Purpose |
| --- | --- |
| [`experiments/`](experiments/) | Twelve guided experiments from first table to regulated-data architecture |
| [`experiments/README.md`](experiments/README.md) | Series index, learning loop, and the 30 interview questions the track covers |
| [`docs/iceberg-lab-spec.md`](docs/iceberg-lab-spec.md) | Build specification for the local sandbox under `lab/` |
| [`lab/`](lab/) | Planned home for Docker Compose, notebooks, and helper code (see spec) |

The experiment write-ups are usable today. The runnable sandbox is specified in [`docs/iceberg-lab-spec.md`](docs/iceberg-lab-spec.md) and will land under `lab/` as implementation proceeds.

## Who this is for

- Data engineers preparing for lakehouse architecture interviews
- Engineers moving from Hive or legacy warehouses to open table formats
- EU FinTech teams who need Iceberg fluency with DORA, BaFin/BAIT, or MiFID II context

This is a learning sandbox, not a production deployment guide. For official Iceberg behavior and APIs, use the [Apache Iceberg documentation](https://iceberg.apache.org/docs/).

## Quick start

### Experiments (available now)

1. Open the series index: [`experiments/README.md`](experiments/README.md)
2. Start with [Experiment 01 — Your first Iceberg table](experiments/01-first-table.md)
3. Keep a personal `interview-faq.md` and answer each experiment's interview question before and after the hands-on work

**Crunch path (about one week):** experiments 01, 02, 04, 05, 07, and 09.

### Local lab (planned)

Target experience once `lab/` is implemented:

```bash
git clone https://github.com/<your-org>/JQ-Apache-Iceberg-Lab.git
cd JQ-Apache-Iceberg-Lab/lab
./init.sh
# open http://localhost:8888 and run notebooks/00_setup_check.ipynb
```

Design goals from the spec: cold start under 60 seconds, full reset under 10 seconds, no Spark or Hive Metastore, and maximum time spent on Iceberg metadata rather than JVM or cluster setup.

## How the experiments are structured

Each experiment follows the same loop:

1. Read the interview question at the top and write your answer first
2. Run the hands-on section in the lab (typing commands, not only pasting)
3. Complete the **Break it** section
4. Read the theory deep-dive with concrete behavior in mind
5. Re-answer the interview question and compare to your first draft

Tiers:

- **Foundation (01–04):** physical layout, metadata, catalogs, non-Spark reads
- **Mechanics (05–08):** snapshots, schema evolution, partitioning, concurrency
- **Production (09–11):** small files, expiration/GC, format comparison
- **Specialization (12):** regulated data and auditability

Experiments 02, 05, 09, 10, and 12 include optional EU regulatory angles.

## Planned sandbox architecture

```
┌─────────────────────────────────────────────┐
│  Host machine                               │
│  ┌──────────────────────────────────────┐   │
│  │  Jupyter Lab (Python 3.11)           │   │
│  │  PyIceberg · DuckDB · PyArrow        │   │
│  └─────────────┬────────────────────────┘   │
│                │                             │
│  ┌─────────────┴──────────┐                 │
│  │  catalog.db (SQLite)   │                 │
│  └─────────────┬──────────┘                 │
│  ┌─────────────┴──────────┐                 │
│  │  MinIO (Docker)        │                 │
│  │  bucket: warehouse     │                 │
│  └────────────────────────┘                 │
└─────────────────────────────────────────────┘
```

Explicit non-goals: Spark, Nessie, Kubernetes, Trino in the sandbox, and any configuration meant for networked or production use.

## Contributing

Issues and pull requests are welcome.

When adding or editing experiments:

- Tie each experiment to at least one explicit interview question
- Include a **Break it** section
- Put hands-on work before theory
- Keep the sandbox JVM-free unless there is a strong, documented reason to change that constraint

## License

- Code in `lab/` (when added): [MIT](LICENSE)
- Written tutorials and specs under `experiments/` and `docs/`: [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)

## Author

Built by Bill (Jiahong) — research in deep learning for aviation, builder of SoloLakehouse, focused on modern data platforms and EU financial regulation.

If this helped you prepare for interviews or ship a lakehouse design, a star or a short issue with what worked (or what confused you) helps others find the material.
