# JQ Apache Iceberg Lab

> Twelve hands-on experiments that take you from "never touched Iceberg" to "can defend Iceberg architecture in a senior data engineer interview" — built around **real interview questions**, with a deliberate **"Break it"** section in every experiment, and an optional **EU regulatory track** (DORA, BaFin/BAIT, MiFID II, GDPR) you won't find in any other Iceberg tutorial.

[![License: MIT](https://img.shields.io/badge/Code-MIT-blue.svg)](LICENSE)
[![Content: CC BY-SA 4.0](https://img.shields.io/badge/Content-CC--BY--SA--4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-sa/4.0/)
[![Status: WIP](https://img.shields.io/badge/Status-Active%20Development-yellow.svg)](#)
[![Made for](https://img.shields.io/badge/Made%20for-Interview%20Prep-orange.svg)](#)

**⭐ If this saves you time, please star the repo — it helps others find the material.**

---

## Why this exists

Most Iceberg tutorials teach you **what commands to run**. They leave you able to follow a script but unable to answer "why does Iceberg's manifest list exist?" in an interview.

This repo is different in three deliberate ways:

1. **Interview-first.** Every experiment opens with a real question a hiring manager will ask. You write your current answer *before* the hands-on work, then again *after*. The delta is what you learned.
2. **"Break it" methodology.** Every experiment has a section dedicated to deliberately breaking something — deleting metadata files, racing concurrent writers, partitioning by high-cardinality keys. Failure modes are where real understanding lives.
3. **EU regulatory track.** Five experiments include optional sections mapping Iceberg's mechanics to DORA Article 28, MiFID II RTS 25, BaFin BAIT, and GDPR Article 17. If you're targeting roles in EU finance (Frankfurt, Amsterdam, Dublin, Luxembourg), this is differentiation you won't find anywhere else.

If those three things match what you need, read on.

---

## Who this is for

- **Data engineers preparing for lakehouse interviews** at companies using Iceberg (Netflix, Apple, Adobe, AWS, Snowflake, Databricks customers, and increasingly EU banks)
- **Engineers migrating from Hive or legacy warehouses** to modern open table formats
- **EU FinTech teams** needing Iceberg fluency with regulatory context built in
- **Anyone who has read the Iceberg docs and still doesn't feel ready** to defend architectural choices

This is a **learning sandbox**, not a production deployment guide. For canonical behavior and APIs, see the [Apache Iceberg documentation](https://iceberg.apache.org/docs/).

---

## What's in this repo

| Path | Purpose | Status |
| --- | --- | --- |
| [`experiments/`](experiments/) | Twelve guided experiments, ~4,200 lines total | ✅ Available |
| [`experiments/README.md`](experiments/README.md) | Series index with all 30 interview questions | ✅ Available |
| [`docs/iceberg-lab-spec.md`](docs/iceberg-lab-spec.md) | Build spec for the local sandbox | ✅ Available |
| [`lab/`](lab/) | Docker Compose + notebooks + helpers | 🚧 In progress |

**The experiments are usable today.** You can run them against any Iceberg environment — the official [Iceberg quickstart docker-compose](https://iceberg.apache.org/spark-quickstart/), an existing setup at your company, or the planned sandbox under [`lab/`](lab/) once it lands.

---

## Time investment

| Path | Duration | Outcome |
| --- | --- | --- |
| **Crunch path** (experiments 01, 02, 04, 05, 07, 09) | ~1 week part-time | Bar for mid-level Iceberg interviews |
| **Full Foundation + Mechanics** (01–08) | ~2 weeks part-time | Bar for senior generalist roles |
| **Full series including specialization** (01–12) | ~3 weeks part-time | Bar for senior platform roles, with EU FinTech differentiation |

Each experiment runs 60–120 minutes. The interview FAQ you build alongside is reusable indefinitely.

---

## The interview questions this prepares you for

A sample of the 30 questions covered. The [full list lives in the series index](experiments/README.md).

**Foundation**
- What does an Iceberg table physically consist of on disk?
- What's the difference between data files, manifests, manifest list, and metadata file?
- Can you read an Iceberg table without Spark? How?

**Mechanics**
- How does Iceberg implement ACID transactions?
- Why is Iceberg schema evolution safe when raw Parquet's isn't? What's a field-id?
- Two writers commit at the same time. What happens?

**Production**
- How do you handle the small file problem?
- What does `expire_snapshots` do? What are its risks?
- Why would you choose Iceberg over Delta Lake?

**Specialization (EU regulatory track)**
- How does Iceberg help with regulatory compliance under DORA?
- If a regulator asks "show me the exact state of this table on date X," how does Iceberg help?
- How would you implement GDPR right-to-be-forgotten on an Iceberg table?

If you can answer all 30 without notes, you are interview-ready for any senior data engineer role involving lakehouse architecture.

---

## Quick start

### 1. Pick your environment

Any of these works:

- **The planned local sandbox** under [`lab/`](lab/) — lightest weight, JVM-free (PyIceberg + DuckDB + MinIO). See [build spec](docs/iceberg-lab-spec.md).
- **The official Iceberg docker-compose** — Spark-based, broader feature coverage. See [Iceberg quickstart](https://iceberg.apache.org/spark-quickstart/).
- **Your company's existing Iceberg setup** — most experiments work against any Iceberg-compliant environment.

### 2. Open the series index

→ [`experiments/README.md`](experiments/README.md)

### 3. Set up your personal interview FAQ

Create a local file called `interview-faq.md`. For each experiment, write your answer to the interview question **before** doing the hands-on work, and again **after**. This testing-effect loop is what makes the series stick.

### 4. Start with Experiment 01

→ [`experiments/01-first-table.md`](experiments/01-first-table.md)

---

## How the experiments are structured

Every experiment follows the same loop:

1. 🎯 Read the **interview question** and write your current answer
2. 🛠️ Do the **hands-on** section (type, don't paste — your fingers learn what your eyes skip)
3. 💥 Do the **Break it** section
4. 📚 Read the **theory deep-dive** with concrete behavior in mind
5. ✍️ **Re-answer** the interview question and compare to your draft
6. 🎁 (Optional) Adapt the included **LinkedIn post draft** to publish what you learned

### Tier structure

| Tier | Experiments | What you'll be able to do |
| --- | --- | --- |
| **Foundation** | 01–04 | Explain Iceberg's physical layout, query plan, and catalog model from memory |
| **Mechanics** | 05–08 | Defend Iceberg's behavior under schema change, partition evolution, and concurrent writes |
| **Production** | 09–11 | Operate Iceberg at scale: small files, GC, format selection |
| **Specialization** | 12 | Defend Iceberg as a compliance architecture choice for EU regulated environments |

Experiments **02, 05, 09, 10, and 12** include optional 🇪🇺 regulatory angle sections.

---

## EU regulatory track (the differentiator)

Most Iceberg tutorials stop at technical mechanics. This series adds a layer that explicitly maps Iceberg's design to specific regulatory requirements relevant to EU financial services.

| Regulation | Article / Section | Iceberg mechanism covered |
| --- | --- | --- |
| **DORA** | Art. 9 (integrity, traceability) | Snapshot summaries, manifest checksums (exp 02) |
| **DORA** | Art. 28 (ICT third-party risk, portability) | Open spec, multi-engine reads (exp 02, 11) |
| **MiFID II** | RTS 22 (transaction reporting fields) | Schema with documented field-ids + table property tagging (exp 12) |
| **MiFID II** | RTS 25 (reproducibility) | End-of-day snapshot tagging + time travel (exp 05, 12) |
| **BaFin BAIT** | Access controls | Catalog-level RBAC (exp 03) |
| **GDPR** | Art. 17 (right to erasure) | Redaction workflow + retention policy (exp 10, 12) |

If you're interviewing in Frankfurt, Amsterdam, Dublin, or Luxembourg — this is the layer that distinguishes you.

---

## Planned sandbox architecture

When [`lab/`](lab/) is implemented, it will provide a 60-second-cold-start environment:

```
┌─────────────────────────────────────────────┐
│  Host machine                               │
│  ┌──────────────────────────────────────┐   │
│  │  Jupyter Lab (Python 3.13)           │   │
│  │  PyIceberg · DuckDB · PyArrow        │   │
│  └─────────────┬────────────────────────┘   │
│                │                            │
│  ┌─────────────┴──────────┐                 │
│  │  catalog.db (SQLite)   │                 │
│  └─────────────┬──────────┘                 │
│  ┌─────────────┴──────────┐                 │
│  │  MinIO (Docker)        │                 │
│  │  bucket: warehouse     │                 │
│  └────────────────────────┘                 │
└─────────────────────────────────────────────┘
```

**Design goals:** cold start < 60s · full reset < 10s · no JVM · zero networked-environment configuration.

**Explicit non-goals:** Spark, Hive Metastore, Nessie, Kubernetes, Trino in the sandbox, or any production-oriented setup. Those belong in production deployments, not learning sandboxes.

Full specification: [`docs/iceberg-lab-spec.md`](docs/iceberg-lab-spec.md).

---

## Contributing

Issues and pull requests are welcome, especially:

- **Corrections** to anything technically wrong
- **Additional Break-it scenarios** that expose interesting failure modes
- **Translations** of experiments into other languages
- **Regulatory mappings** for jurisdictions outside the EU (US SEC, UK FCA, MAS, etc.)

Style guide for contributions:

- Every experiment must tie to at least one explicit interview question
- Every experiment must include a **Break it** section
- Hands-on work comes **before** theory, not after
- Keep the sandbox JVM-free unless there's a strong, documented reason to change that constraint

---

## License

- Code in `lab/` (when added): [MIT](LICENSE)
- Tutorials and specs under `experiments/` and `docs/`: [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)

You are free to use this material for personal learning, team training, internal workshops, or as the basis for derivative tutorials — provided you give attribution and share derivatives under the same license.

---

## About the author

Built by **Jiahong** — PhD candidate in deep learning applied to aviation operations, builder of [SoloLakehouse](https://github.com/) (a self-hosted, compliance-first lakehouse for EU FinTech), based in Frankfurt am Main.

I write at the intersection of modern data platforms, EU financial regulation, and platform engineering. If this material helped you, the most useful things you can do are:

- ⭐ **Star the repo** so others can find it
- 💬 **Open an issue** with what worked or what confused you
- 🤝 **Connect on [LinkedIn](https://www.linkedin.com/)** — I'm especially happy to talk with engineers and hiring managers at Frankfurt-area FinTechs, EU banks, or anyone building compliance-aware data platforms

If you're hiring for platform engineering or senior data engineering roles in Frankfurt or remote-EU, [I'd love to hear from you](https://www.linkedin.com/).

---

*Last updated: May 2026 · Apache Iceberg version targeted: 1.5+ / spec v2*