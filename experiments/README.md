# Apache Iceberg: From Zero to Interview-Ready

> A hands-on experiment series that takes you from "never heard of Iceberg" to "ready to defend Iceberg architecture decisions in a senior data engineer interview."
>
> Built on top of the [iceberg-lab](../) sandbox — a 60-second-startup local environment using PyIceberg + DuckDB + MinIO.

---

## Why this series exists

Most Iceberg tutorials teach you **what commands to run**. They leave you able to follow a script but unable to answer "why does Iceberg's manifest list exist?" in an interview.

This series is different. Every experiment is built around **a question an interviewer will actually ask you**. You don't just run code — you break things, inspect raw metadata files by hand, and write down your own answer before reading mine.

**Who this is for:**
- Data engineers preparing for interviews at companies using lakehouse architectures (DBG, Trade Republic, Commerzbank Digital, Databricks customers, AWS data platform teams, etc.)
- Engineers transitioning from Hive/legacy warehouses to modern table formats
- EU FinTech engineers who need to understand Iceberg through a regulatory lens (DORA, BaFin/BAIT, MiFID II)

**Who this is NOT for:**
- People looking for production deployment guides (use the [Iceberg docs](https://iceberg.apache.org/docs/) for that)
- People who want to learn Spark — this series uses PyIceberg + DuckDB on purpose, to keep the focus on Iceberg itself, not on JVM tuning

---

## How to use this series

**Lab lifecycle (clone → run → shut down):** see [`docs/operation-guide.md`](../docs/operation-guide.md) §0. That guide covers `./init.sh`, the per-experiment loop below, `./reset.sh --confirm`, end-of-day `docker compose stop`, and full cleanup.

**Recommended pace:** 1 experiment per evening (90–120 min each). Full series = ~3 weeks part-time.

**For interview prep crunch (1 week):** Do experiments 01, 02, 04, 05, 07, 09. Skip the rest on first pass.

**For each experiment, follow this loop:**

1. **Read the "Interview question" at the top.** Write your current answer in a personal `interview-faq.md` — even if it's "I don't know." This is the most important step. Skipping it means you're consuming, not learning.
2. **Do the hands-on section.** Don't copy-paste — type it. Your fingers learn what your eyes skip.
3. **Do the "Break it" section.** This is where real understanding lives.
4. **Read the "Theory deep-dive" section.** Now it actually means something because you have a memory of the behavior.
5. **Re-answer the interview question.** Compare to your draft from step 1. The delta is what you learned.
6. **Follow the Session wrap-up** at the end of each experiment (reset, end-of-day stop, or full cleanup).
7. **(Optional but recommended)** Write a 200-word LinkedIn post about one insight. Public commitment compounds.

---

## The experiment map

The series has four tiers. Each tier builds the answer to a different category of interview question.

### Tier 1 — Foundation: "What IS Iceberg?"
For people who can describe what an Iceberg table looks like and how reads/writes physically happen.

| # | Experiment | Core interview question |
|---|---|---|
| 01 | [Your first Iceberg table](01-first-table.md) | "What does an Iceberg table physically consist of?" |
| 02 | [Anatomy of metadata](02-metadata-anatomy.md) | "Walk me through what happens when you query an Iceberg table." |
| 03 | [The catalog layer](03-catalog-layer.md) | "What does a catalog do, and why does Iceberg need one?" |
| 04 | [Reads without Spark](04-reads-without-spark.md) | "How would you query Iceberg from a Python service?" |

### Tier 2 — Mechanics: "How does Iceberg work under pressure?"
For people who can defend Iceberg's behavior under concurrency, evolution, and failure.

| # | Experiment | Core interview question |
|---|---|---|
| 05 | [Snapshots & time travel](05-snapshots-time-travel.md) | "How does Iceberg implement ACID?" |
| 06 | [Schema evolution](06-schema-evolution.md) | "Why is Iceberg schema evolution safe but Parquet's isn't?" |
| 07 | [Hidden partitioning](07-hidden-partitioning.md) | "What's hidden partitioning and why does it matter?" |
| 08 | [Concurrent writes & isolation](08-concurrent-writes.md) | "Two writers commit at the same time. What happens?" |

### Tier 3 — Production: "Can you operate Iceberg at scale?"
For people who can be trusted with a production lakehouse.

| # | Experiment | Core interview question |
|---|---|---|
| 09 | [The small files problem](09-small-files-compaction.md) | "How do you handle the small file problem?" |
| 10 | [Snapshot expiration & GC](10-expiration-gc.md) | "What are the risks of `expire_snapshots`?" |
| 11 | [Iceberg vs Delta vs Hudi](11-iceberg-vs-delta-vs-hudi.md) | "Why would you choose Iceberg over Delta?" |
| 13 | [MinIO vs SeaweedFS](13-minio-vs-seaweedfs.md) | "How do you choose object storage for Iceberg? What does Iceberg require from S3?" |

### Tier 4 — Specialization: "Can you defend architectural decisions?"
The differentiation layer. Few candidates get here. This is where €70k offers become €100k+ offers.

| # | Experiment | Core interview question |
|---|---|---|
| 12 | [Iceberg for regulated data (DORA / MiFID II)](12-iceberg-for-regulated-data.md) | "How does Iceberg help with regulatory compliance?" |

---

## The 30 interview questions this series prepares you for

A flat list. If you can answer all 33 without notes, you are interview-ready for any senior data engineer role that involves lakehouse architecture.

**Foundation:**
1. What does an Iceberg table physically consist of on disk?
2. What's the difference between data files, manifests, manifest list, and metadata file?
3. Why doesn't Iceberg use the filesystem directory structure to find data (like Hive does)?
4. What does an Iceberg catalog do? Why is it needed?
5. What catalog implementations exist and how do they differ?
6. Can you read an Iceberg table without Spark? How?

**Mechanics:**
7. How does Iceberg implement ACID transactions?
8. What is a snapshot? How does time travel work?
9. What happens to old snapshots — are they deleted automatically?
10. Why is Iceberg schema evolution safe when raw Parquet's isn't?
11. What's a field-id and why does it matter?
12. What's hidden partitioning? What problem does it solve?
13. Can you change a table's partitioning without rewriting all data? How?
14. Two writers commit to the same table at the same time. What happens?
15. What's optimistic concurrency control in Iceberg? When does it fail?

**Production:**
16. What's the small file problem? How does Iceberg help (or not)?
17. How does compaction work? What are the tradeoffs of different strategies?
18. What does `expire_snapshots` do? What are its risks?
19. How do you delete data from an Iceberg table? (Copy-on-write vs merge-on-read)
20. How do you monitor an Iceberg table in production? What metrics matter?
21. How does Iceberg compare to Delta Lake on transaction protocol?
22. How does Iceberg compare to Hudi on streaming workloads?
23. When would you NOT choose Iceberg?
31. How do you choose object storage (MinIO, SeaweedFS, S3) for an Iceberg lakehouse?
32. What S3 API semantics does Iceberg depend on for commits and reads?
33. Why does the small-file problem stress object storage LIST operations, not just query engines?

**Specialization:**
24. How does Iceberg's snapshot model support auditability requirements like DORA Article 28?
25. If a regulator asks "show me the exact state of this table on date X," how does Iceberg help?
26. How would you implement GDPR right-to-be-forgotten on an Iceberg table?
27. What's the storage cost of retaining 7 years of snapshots?
28. How do you prove to an auditor that data hasn't been tampered with?
29. What are the security boundaries in an Iceberg deployment?
30. How would you migrate from Hive to Iceberg in a production environment with zero downtime?

---

## The "EU FinTech track" (optional sub-series)

If you're targeting roles in EU regulated finance, these experiments have additional **🇪🇺 Regulatory angle** sections. The technical content is identical; the regulatory sections are bonus signal for interviews at DBG, Trade Republic, Commerzbank, ING, KBC, etc.

Experiments with regulatory angle: **02, 05, 09, 10, 12.**

---

## Contributor Policy

This series belongs to a single-author repository. The only allowed Git contributor is:

```text
Jiahong Que <jiahongque25@gmail.com>
```

Feedback and corrections are welcome through GitHub issues, but commits must remain authored only by Jiahong Que. Code agents must follow [`../AGENTS.md`](../AGENTS.md) and verify contributor identity before committing or pushing.

**Style guide for experiment changes:**
- Every experiment must answer at least one interview question explicitly
- Every experiment must have a "Break it" section
- Theory comes AFTER hands-on, never before
- No JVM-based tools (this is a deliberate constraint to keep the sandbox light)

---

## License

MIT for code. CC-BY-SA 4.0 for the written content.

---

## About the author

Built by [Jiahong Que](#) — PhD candidate in deep learning for aviation, builder of [SoloLakehouse](#), focused on the intersection of modern data platforms and EU financial regulation.

If you're hiring for platform / data engineering roles in Frankfurt or remote-EU, [let's talk](#).
