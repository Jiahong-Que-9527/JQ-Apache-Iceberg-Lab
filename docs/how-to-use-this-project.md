# How to Use This Project

Use this repo as a learning path, not as a random pile of notebooks. The documents and code are meant to work together:

| Start here when... | Read / run | Why |
| --- | --- | --- |
| You are new to the repo | [`README.md`](../README.md) | Portfolio overview, learning paths, and quick start |
| You want the day-to-day commands | [`docs/operation-guide.md`](operation-guide.md) | Clone, start, verify, reset, stop, cleanup, troubleshooting |
| You want the experiment list | [`experiments/README.md`](../experiments/README.md) | All 21 experiments, interview questions, tier map |
| You are doing a lab session | One file under [`experiments/`](../experiments/) plus its notebook or shell steps | The Markdown gives the learning loop; the notebook/code gives the evidence |
| You need JVM-only Iceberg features | [`lab/spark-profile/README.md`](../lab/spark-profile/README.md) | Spark, Trino, and Lakekeeper for experiments 14, 16, 17, 20, 21 |
| You are changing the lab itself | [`docs/iceberg-lab-spec.md`](iceberg-lab-spec.md) | Implementation contract for the lightweight lab and optional Spark profile |

## Recommended Study Flow

1. Read the root [`README.md`](../README.md) once.
2. Follow [`docs/operation-guide.md`](operation-guide.md) sections 1-4 to start the main lab and run the setup notebooks.
3. Open [`experiments/README.md`](../experiments/README.md) and pick a path:
   - Interview crunch: 01, 02, 04, 05, 07, 09
   - Production crunch: 01, 02, 03, 05, 07, 08, 09, 10, 14, 15, 17, 18, 19
   - Full series: 01-21, with 12 as the EU regulated-data specialization
4. For every experiment, use the same loop:
   - Write a before answer in local `interview-faq.md`
   - Run the hands-on section
   - Do the Break it section
   - Read the theory
   - Rewrite the answer
5. Use [`docs/operation-guide.md`](operation-guide.md) section 5.1 to match each experiment to the right notebook or runtime.

## Runtime Choice

Use the main lab by default:

```bash
cd lab
./init.sh
```

This starts Jupyter, MinIO, SeaweedFS, and the SQLite catalogs. It covers experiments 01-13, 15, 18, and 19.

Only start the Spark profile when an experiment asks for it:

```bash
cd lab/spark-profile
./up.sh
```

That profile adds Spark, Trino, Lakekeeper, and Postgres for experiments 14, 16, 17, 20, and 21.

## Local Files

Keep personal notes local:

- `interview-faq.md`
- ad hoc notebooks
- scratch SQL files

Do not commit runtime state:

- `lab/.env`
- `lab/catalog*.db`
- `lab/warehouse*/`
- `lab/spark-profile/state/`
- notebook checkpoints and caches
