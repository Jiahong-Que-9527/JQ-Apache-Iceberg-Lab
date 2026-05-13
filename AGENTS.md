# Code Agent Instructions

This repository is a single-author project.

Mandatory contributor identity:

- The only allowed Git contributor is `Jiahong Que <jiahongque25@gmail.com>`.
- All commits made by code agents must use that exact author identity.
- Do not add `Co-authored-by`, generated-by, bot attribution, or any other trailer that would create another contributor identity.
- Do not preserve external patch authorship in commits. If external suggestions are used, rewrite the final commit under the required author identity.
- Before committing and before pushing, run:

```bash
git config user.name
git config user.email
git shortlog -sne --all
```

The expected contributor output is:

```text
Jiahong Que <jiahongque25@gmail.com>
```

If any other contributor appears, stop and ask Jiahong before continuing.

Project rules:

- Keep the tutorial focused on Apache Iceberg learning and senior data-engineering interview preparation.
- Keep the local lab lightweight: PyIceberg, DuckDB, MinIO, SQLite catalog; avoid Spark, Hive Metastore, Nessie, Kubernetes, and Trino unless a document explicitly explains why.
- Keep experiments hands-on first, theory second, with a deliberate "Break it" section.
- Do not commit runtime state such as `lab/catalog.db`, `lab/warehouse/`, `.env`, notebook checkpoints, caches, or local interview notes.

