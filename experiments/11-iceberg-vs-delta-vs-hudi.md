# Experiment 11 — Iceberg vs Delta Lake vs Hudi

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Tiers 1–2 complete
>
> **Format note:** This experiment is heavier on theory and lighter on hands-on than the others. The work is to internalize a decision framework you can defend in interviews.

---

## 🎯 The interview question this answers

> **"Why would you choose Iceberg over Delta Lake? When would you NOT?"**

Write your current answer in `interview-faq.md`. Be honest — most people can't answer this well.

**Why this matters:** This is the single most predictable interview question for any senior lakehouse role in 2026. Most candidates give a memorized "Iceberg is open and Delta is Databricks" answer. The good answer is much more interesting and shows you understand the ecosystem dynamics.

---

## TL;DR

All three (Iceberg, Delta, Hudi) solve the same problem: ACID + schema evolution + time travel on top of object storage. They differ in **transaction protocol design, ecosystem alignment, and operational characteristics**. None is universally better. The right answer depends on engine choice, vendor strategy, and workload type.

---

## 🛠️ Hands-on: minimal comparison experiments

### Step 1 — Same data, three engines, observed differences

We can only run Iceberg in this lab (no JVM = no Delta or Hudi). But we can read public documentation comparisons and inspect Iceberg's behavior to ground the theory.

```python
from src.catalog_helper import get_catalog
catalog = get_catalog()

# Confirm what we already know about Iceberg
table = catalog.load_table("lab.gc_demo")  # or any from earlier experiments

print("Iceberg-specific properties:")
print(f"  Spec version: 2 (most production)")
print(f"  Catalog: SQLite (one of many)")
print(f"  Snapshot count: {len(list(table.snapshots()))}")
print(f"  Has refs (tags/branches): {bool(table.refs)}")
print(f"  Manifest structure: list of manifest files")
```

### Step 2 — Compare commit semantics by experiment

In Experiment 08 you saw Iceberg's OCC with retry. Let's note what Delta and Hudi do differently (you can't run this here):

| | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| Transaction log location | Catalog (atomic pointer) | `_delta_log/` folder inside table | `.hoodie/timeline/` folder |
| Atomic primitive | Catalog conditional update | Single file rename on storage with put-if-absent semantics | Timeline file rename |
| Concurrency model | OCC with retry | OCC with retry | OCC with retry (slightly different mechanism) |
| Multi-table transactions | Only via Nessie | No | No |

**Insight:** all three use OCC. The difference is **where they store the transaction log**: outside the storage layer (Iceberg, via catalog) vs. inside (Delta, Hudi). This single choice has cascading consequences:

- Iceberg + S3 works perfectly (no PUT-if-absent needed at storage layer)
- Delta + S3 had problems for years until S3 added conditional writes (2024)
- Hudi + S3 needs an external lock provider for safety

### Step 3 — Spec governance comparison

Read these three pages briefly (open browser):

- Iceberg spec: https://iceberg.apache.org/spec/
- Delta spec: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- Hudi: https://hudi.apache.org/docs/

Note governance differences:

- **Iceberg** — Apache project, governed by Apache Software Foundation. Multi-vendor TSC. Major contributors: Netflix (origin), Apple, Adobe, AWS, Tabular (acquired by Databricks), Snowflake (Polaris).
- **Delta Lake** — Apache project as of 2022, but heavily Databricks-influenced. Open source spec, but the most advanced features land in Databricks Runtime first.
- **Hudi** — Apache project, Uber origin. Smaller community than the other two.

**This governance picture matters for vendor lock-in concerns.** If you're a bank evaluating long-term commitments, "neutral foundation governance" is a real attribute, not marketing.

---

## 💥 Break it

This experiment is decision-framework heavy, so the breakage is about breaking bad arguments rather than corrupting a table.

### Break 1: make the lazy Iceberg argument

Write this answer in your notes:

> "I would choose Iceberg because it is open source and avoids Databricks lock-in."

Now attack it:

- Delta Lake is also open source.
- Delta has strong non-Databricks readers.
- Databricks supports Iceberg reads through UniForm.
- A self-hosted Iceberg catalog can still become an operational bottleneck.

**Insight:** "open" is not enough. A senior answer must name concrete requirements: engine fleet, catalog ownership, schema evolution, governance, write patterns, and vendor strategy.

### Break 2: choose Delta for the wrong workload

Assume a platform with Spark, Trino, DuckDB, Python services, Snowflake external reads, and no Databricks contract. Force yourself to choose Delta anyway. What has to be true?

You should end up with a short list: mature Delta readers across all engines, a catalog story that everyone can share, a security model outside Unity Catalog, and tested behavior for generated columns / UniForm / protocol features. If you cannot prove those, the choice is mostly wishful thinking.

### Break 3: choose Iceberg for the wrong workload

Now assume the opposite: one Databricks workspace, Spark Structured Streaming, heavy `MERGE INTO`, tight Databricks governance, and no independent Trino/Snowflake readers. Force yourself to choose Iceberg anyway.

You should feel the tradeoff immediately: you are swimming away from the platform's native path. Iceberg may still be defensible for long-term portability, but it is no longer the obvious operational choice.

### Break 4: test your decision tree

Take three real companies or teams you know. For each one, fill this in:

```text
Team:
Engines:
Catalog / governance:
Write pattern:
Vendor constraints:
Recommended format:
Why not the other two:
```

If your recommendation is always Iceberg, your framework is broken.

---

## 📚 Theory deep-dive

### The honest comparison

| Dimension | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| Transaction protocol | Manifest list/files + catalog | `_delta_log/` JSON + checkpoints | Timeline-based |
| Schema evolution | Field-id based, very safe | Name-based, with restrictions | Name-based, with restrictions |
| Hidden partitioning | Yes, with evolution | Generated columns (similar idea) | Some support |
| Concurrency | OCC, snapshot isolation | OCC, snapshot isolation | OCC, multiple isolation levels |
| Time travel | Snapshot-based, branches & tags | Version/timestamp-based | Timeline-based |
| Row-level operations | v2 spec: copy-on-write + merge-on-read | Both modes; MERGE INTO mature | Built around upserts; MoR-first |
| Streaming ingestion | Good (Flink, Spark Streaming, Kafka Connect) | Excellent (Spark Structured Streaming first-class) | Excellent (designed for streaming upserts) |
| Engine ecosystem | Spark, Flink, Trino, DuckDB, Polars, Snowflake, BigQuery, ClickHouse, Redshift | Spark first (esp Databricks), Trino, Flink, growing | Spark, Flink, Trino |
| Catalog options | Many (Hive, Glue, Nessie, REST, Polaris, Lakekeeper) | Limited (Hive, Unity Catalog, file-based) | Limited |
| Cloud-native multi-engine reads | Best in class | Good (improving) | Spark-centric |
| Databricks integration | Via Uniform | Native, best-in-class | External |
| Snowflake integration | Native (via Polaris and managed Iceberg) | External | External |

### When Iceberg wins

1. **Multi-engine architecture.** If you have Spark + Trino + DuckDB + something else all reading the same data, Iceberg's catalog/spec maturity is unmatched.
2. **Vendor neutrality matters.** Especially for regulated industries (EU finance) that need to avoid lock-in.
3. **Long-term schema evolution.** Field-id-based evolution is genuinely safer than Delta's name-based.
4. **Production-grade hidden partitioning with evolution.** Iceberg's partition evolution is uniquely strong.
5. **AWS-native shops who want open standards.** Glue + Iceberg + Athena/EMR is a mature stack.

### When Delta wins

1. **You're a Databricks customer.** Period. Native integration, best performance, best tooling.
2. **MERGE INTO and CDC workflows.** Delta's MERGE has been production-mature longer.
3. **Streaming-first architectures with Spark.** Structured Streaming + Delta is the original combo.
4. **Single-engine, single-vendor strategic alignment.** Some shops want this and Databricks delivers.

### When Hudi wins

1. **Upsert-heavy streaming workloads.** Hudi was designed for this; Iceberg and Delta have caught up but Hudi still has nuances.
2. **Sub-second commit needs.** Hudi's timeline model can be tuned for very fast commits.
3. **Specific Uber-like patterns** (large-scale data product pipelines with complex SLAs).

### The vendor dynamics (the part most candidates miss)

The competitive landscape shifted dramatically in 2024:

- **Databricks acquired Tabular** (the company founded by Iceberg's original creators) for ~$1B
- **Snowflake launched Polaris** (open-source Iceberg catalog) and aggressively positions Iceberg as the open format
- **Databricks responded with Unity Catalog open-sourcing** and "Delta UniForm" (Delta files readable as Iceberg)
- **AWS, Google, Microsoft all aligned on Iceberg** for their lakehouse offerings

**Net effect: Iceberg has become the de facto open lakehouse format**, with Delta's main moat being the Databricks platform integration. This is shifting interview questions away from "which is better technically" to "what's your vendor and platform strategy."

In interviews, mentioning this strategic context = senior-level signal.

### The "Delta UniForm" trap question

Some interviewers will ask: "Doesn't UniForm make the Iceberg vs Delta question moot?"

Good answer: "UniForm lets Delta tables be read as Iceberg, which is a thoughtful middle ground for hybrid environments. But it's read-only and adds metadata-writing overhead. For greenfield architectures targeting multi-engine ecosystems, native Iceberg is still simpler. UniForm is a migration helper, not a unification."

### Decision tree

For interviews, have this decision tree ready:

```
Are you all-in on Databricks?
├── Yes → Delta
└── No
    ├── Do you have a strong streaming-upsert pattern?
    │   ├── Yes, with deep Hudi expertise → Hudi (rare but valid)
    │   └── Otherwise → Iceberg
    └── (Most non-Databricks shops) → Iceberg
```

This is the answer that hiring managers respect.

---

## 🇪🇺 Regulatory angle

For EU FinTech specifically, the format choice has additional dimensions:

**Vendor neutrality (DORA Article 28).** A bank cannot afford to be locked into a single vendor's data format. Iceberg's multi-vendor TSC and broad engine support is **the most defensible choice on the regulatory dimension**. Delta is increasingly multi-vendor but its governance still skews Databricks. Hudi has less corporate alignment.

**Cloud sovereignty.** EU banks often need to demonstrate they can move data between clouds. Iceberg + open catalogs (Lakekeeper, Polaris, Nessie) gives the strongest story here. Delta + Unity Catalog ties you to specific catalog implementations.

**Audit and inspection.** All three formats are open and inspectable. Iceberg's manifest model is arguably the easiest to audit at the file level (we did this in Experiment 02). For regulators who want to verify data integrity by inspection, this matters.

**Bottom line for EU interviews:** if asked "why Iceberg over Delta?" in a regulated context, your answer should emphasize **vendor neutrality, catalog flexibility, and inspectability** — not just technical features. **This framing alone differentiates you from 90% of candidates.**

---

## ✍️ Re-answer the interview question

> "Why would you choose Iceberg over Delta? When would you not?"

Your answer should cover:

1. The technical convergence: all three solve the same core problem, with similar OCC and snapshot models
2. The architectural difference: Iceberg's transaction log is in the catalog; Delta's is in the storage layer; this cascades into operational characteristics
3. Iceberg's distinguishing strengths: multi-engine, vendor-neutral governance, field-id-based schema evolution, partition evolution
4. Delta's distinguishing strengths: Databricks-native, MERGE INTO maturity, streaming-with-Spark heritage
5. The strategic landscape: 2024 vendor moves; Iceberg as the open standard
6. A clear conditional answer: "for greenfield, non-Databricks, multi-engine → Iceberg. For Databricks-aligned → Delta. For specific upsert-streaming → Hudi."

Aim for 90–120 seconds. This is a multi-part answer and interviewers expect depth.

---

## 🎁 LinkedIn post draft

> **"Iceberg vs Delta vs Hudi in 2026: the technical comparison has gotten boring. The vendor dynamics haven't. Here's the strategic landscape I walk hiring managers through."**
>
> This is high-engagement content. The vendor dynamics angle is undercovered. Include the decision tree.

---

## Production format checkpoint ✅

You've finished the format-selection checkpoint of the production tier. Re-answer the production-format questions in your `interview-faq.md` without notes. If anything is fuzzy, redo.

**Self-check question:** can you explain why a non-Databricks, multi-engine platform often picks Iceberg without pretending Delta or Hudi are bad tools? If yes, your answer is senior-shaped.

The next experiments move from format strategy into storage choice, regulated-data design, and production operations.

---

## Session wrap-up (close the loop)

1. Confirm hands-on is done: comparison table / decision tree internalized, **after** re-answer written (90–120 s spoken). Optional notebook: `lab/notebooks/07_iceberg_vs_delta.ipynb`.
2. Update `interview-faq.md` with your **after** answer to this experiment's interview question and the format-selection self-check.
3. If you experimented with broken catalog/storage state elsewhere: `cd lab && ./reset.sh --confirm` — [operation guide §7](../docs/operation-guide.md).
4. **End of day** (pause until tomorrow, keep data): `cd lab && docker compose stop` — [operation guide §8](../docs/operation-guide.md).
5. **Done with the lab on this machine** (remove all local tables and catalogs): [operation guide §10](../docs/operation-guide.md).

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md).

---

## Next up

→ [Experiment 13: MinIO vs SeaweedFS](13-minio-vs-seaweedfs.md) — object storage for Iceberg, then [Experiment 12: Iceberg for Regulated Data](12-iceberg-for-regulated-data.md).
