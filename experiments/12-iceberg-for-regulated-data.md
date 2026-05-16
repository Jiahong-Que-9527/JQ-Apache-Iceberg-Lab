# Experiment 12 — Iceberg for Regulated Data (DORA, MiFID II, GDPR)

> **Time:** 120 min · **Tier:** Specialization · **Prerequisites:** Tiers 1–3 complete; [Experiment 13](13-minio-vs-seaweedfs.md) recommended for storage portability (DORA Art. 28)
>
> ⭐ **This is the differentiator.** Tier 1–3 gets you to senior-generalist level. This experiment is what separates a €70-85k offer from a €90-110k offer at an EU bank.

---

## 🎯 The interview question this answers

> **"How does Iceberg help with regulatory compliance? Walk me through how you'd design a lakehouse for a bank subject to DORA, MiFID II, and GDPR."**

Write your current answer in `interview-faq.md`. **Especially be honest here** — most candidates have no answer at all. That's why this question is decisive.

**Why this matters:** EU FinTech roles increasingly require regulatory awareness, but most data engineers approach regulation as "compliance team's problem." Engineers who can speak to both the technical and regulatory layers simultaneously are scarce and expensive. **This is your scarce node positioning.**

---

## TL;DR

EU financial regulation imposes specific data-handling requirements: traceability (DORA), reproducibility (MiFID II RTS 25), portability (DORA Article 28), erasure (GDPR Article 17), retention (multiple), and integrity (DORA Article 9). Iceberg's design — snapshots, tags, field-ids, open spec, manifest-level audit trails — maps directly onto these requirements. **But not perfectly.** This experiment makes you fluent in both the alignments and the gaps.

---

## 🛠️ Hands-on: a compliance scenario walkthrough

### Step 1 — The scenario

You're the lead data engineer at a Frankfurt-based fintech. You're building a transactions table that will be used for:

- Real-time fraud detection (operational)
- End-of-day regulatory reporting (MiFID II)
- Quarterly audits (BaFin)
- Customer-facing transaction history (GDPR-affected)

The table holds personally-identifying information. Retention requirements are conflicting: MiFID II says 5+ years; GDPR says "no longer than necessary." Auditors will ask: "show me the exact state of this table on date X."

You will spec the Iceberg setup that makes this work.

### Step 2 — Schema with regulatory awareness

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, TimestampType, DoubleType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, BucketTransform

catalog = get_catalog()
catalog.create_namespace_if_not_exists("regulated")

schema = Schema(
    NestedField(1, "txn_id", StringType(), required=True,
                doc="Globally unique transaction identifier. MiFID II RTS 22 field."),
    NestedField(2, "txn_ts", TimestampType(), required=True,
                doc="UTC timestamp. Microsecond precision per MiFID II RTS 25."),
    NestedField(3, "user_id", LongType(), required=True,
                doc="Internal user identifier. GDPR-relevant (pseudonymous)."),
    NestedField(4, "counterparty_id", LongType(),
                doc="Counterparty institution LEI mapping."),
    NestedField(5, "amount_eur", DoubleType(), required=True,
                doc="Transaction amount in EUR."),
    NestedField(6, "txn_type", StringType(), required=True,
                doc="One of: BUY, SELL, TRANSFER, FEE."),
    NestedField(7, "country", StringType(),
                doc="ISO 3166-1 alpha-2. Used for jurisdictional analysis."),
    NestedField(8, "venue_id", StringType(),
                doc="MiFID II RTS 22 trading venue MIC."),
)
```

**Notice the `doc` strings.** In a regulatory context, **schema documentation IS regulatory documentation.** Iceberg supports per-field docs. Use them. This appears in metadata.json and is queryable.

### Step 3 — Partitioning aligned with retention and access patterns

```python
partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="txn_day"),
    PartitionField(source_id=3, field_id=1001, transform=BucketTransform(16), name="user_bucket"),
)

table = catalog.create_table("regulated.transactions", schema=schema, partition_spec=partition_spec,
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.format.default": "parquet",
        # Regulatory tagging via table properties:
        "regulatory.classification": "MiFID-II-RTS-22",
        "regulatory.retention_min_years": "7",
        "regulatory.contains_pii": "true",
        "regulatory.lawful_basis": "Art.6(1)(b)-contract",
    })
```

**The properties section is gold.** Iceberg's free-form table properties become your regulatory metadata layer. Auditors can query: "show me all tables tagged as MiFID-II-RTS-22 with retention >= 7 years." You can build governance tooling on top.

### Step 4 — Write some sample data

```python
import pyarrow as pa
from datetime import datetime, timezone, timedelta
import random

def make_batch(date, n=100):
    return pa.table({
        "txn_id":          [f"T{date.strftime('%Y%m%d')}_{i:06d}" for i in range(n)],
        "txn_ts":          [date + timedelta(seconds=random.randint(0, 86400)) for _ in range(n)],
        "user_id":         [random.randint(1, 10000) for _ in range(n)],
        "counterparty_id": [random.randint(1, 500) for _ in range(n)],
        "amount_eur":      [round(random.uniform(10, 5000), 2) for _ in range(n)],
        "txn_type":        [random.choice(["BUY", "SELL", "TRANSFER", "FEE"]) for _ in range(n)],
        "country":         [random.choice(["DE", "NL", "FR", "IT", "ES"]) for _ in range(n)],
        "venue_id":        [random.choice(["XETR", "XPAR", "XLON"]) for _ in range(n)],
    })

base = datetime(2026, 5, 10, tzinfo=timezone.utc)
for day in range(5):
    table = catalog.load_table("regulated.transactions")
    table.append(make_batch(base + timedelta(days=day)))
print("Wrote 5 days of transactions.")
```

### Step 5 — Regulatory tagging: end-of-day snapshots

This is the **single most important pattern** for MiFID II compliance:

```python
table = catalog.load_table("regulated.transactions")

# At end of each business day, tag the snapshot
# (In production: scheduled job at 23:59 UTC)
current_snapshot = table.current_snapshot()
tag_name = f"eod-{base.strftime('%Y-%m-%d')}"

table.manage_snapshots().create_tag(
    snapshot_id=current_snapshot.snapshot_id,
    tag_name=tag_name,
).commit()

table = catalog.load_table("regulated.transactions")
print(f"Created EOD tag: {tag_name}")
print(f"All refs: {list(table.refs.keys())}")
```

**This single tag is your regulatory baseline.** A year from now, when an auditor asks "show me the state of this table at end-of-day on 2026-05-10," you have an exact, immutable, byte-for-byte answer.

### Step 6 — Demonstrate the audit query

```python
# An auditor asks: "show me all DE-country BUY transactions over EUR 1000 as of EOD May 10."
result = table.scan(
    snapshot_id=table.refs[tag_name].snapshot_id,
    row_filter="country = 'DE' AND txn_type = 'BUY' AND amount_eur > 1000",
).to_arrow()

print(result.to_pandas().head())
```

**This is exact reproducibility.** Run it today, run it in 7 years — same data, same result. **This is MiFID II RTS 25 satisfied in 5 lines of code.**

### Step 7 — GDPR deletion workflow

A user submits a GDPR Article 17 deletion request.

```python
# Step 1: identify the data
user_to_delete = 42
affected = table.scan(row_filter=f"user_id = {user_to_delete}").to_arrow()
print(f"Affected rows: {len(affected)}")
print(f"Affected partitions: {set(d.strftime('%Y-%m-%d') for d in affected['txn_ts'].to_pandas())}")

# Step 2: redact at the current snapshot
# This is where it gets tricky. Iceberg's delete API depends on version.
# In production:
#   table.delete(delete_filter=f"user_id = {user_to_delete}")
# Or:
#   spark.sql(f"DELETE FROM regulated.transactions WHERE user_id = {user_to_delete}")

# Step 3: log the deletion request
deletion_log = {
    "request_id": "GDPR-2026-05-13-001",
    "user_id": user_to_delete,
    "received_date": "2026-05-13",
    "executed_date": "2026-05-13",
    "affected_snapshots": [s.snapshot_id for s in table.snapshots()],
    "affected_files_count": len(set(f.file.file_path for f in table.scan(row_filter=f"user_id = {user_to_delete}").plan_files())),
}
# Persist this log to a separate "GDPR audit log" table.

# Step 4: schedule retention enforcement
# When this user's data was last in the table is recorded.
# Old snapshots containing this user's data must be expired by:
#   max(deletion_date) + GDPR_GRACE_PERIOD (typically 30 days)
```

**The hard part of GDPR + Iceberg** is that historical snapshots retain the deleted user's data. Three approaches:

1. **Short snapshot retention** (e.g., < 30 days). Simple. Sacrifices time travel.
2. **Per-partition rewrite for affected partitions.** Run a job that rewrites only partitions containing the user. Expire old snapshots of those partitions.
3. **Encryption-with-key-deletion.** Encrypt user data with per-user keys. GDPR deletion = delete the key. Data becomes unreadable. Highly compliant but complex.

In interviews, articulate the trade-offs explicitly. "We use approach 2 with a 30-day SLA, documented in our GDPR procedure, with affected snapshots flagged in our audit log."

### Step 8 — Build an audit query helper

```python
def regulatory_audit_query(table, date_str, filter_expr=None):
    """Return the table state at end of day for the given date, suitable for audit."""
    tag = f"eod-{date_str}"
    if tag not in table.refs:
        raise ValueError(f"No EOD tag for {date_str}. Either the date wasn't tagged or has been expired.")

    scan = table.scan(snapshot_id=table.refs[tag].snapshot_id)
    if filter_expr:
        scan = scan.filter(filter_expr)
    return scan.to_arrow()

# Example audit:
result = regulatory_audit_query(table, "2026-05-10", "txn_type = 'BUY'")
print(f"Audit result: {len(result)} BUY transactions on 2026-05-10")
```

**This helper is the kind of small abstraction that hiring managers love to see.** It demonstrates that you think about *operationalizing* regulatory work, not just enabling it.

---

## 💥 Break it

### Break 1: expire a regulatory tag

```python
# Drop the EOD tag — simulating accidental deletion
table.manage_snapshots().remove_tag(tag_name).commit()
table = catalog.load_table("regulated.transactions")

# Try the audit query
try:
    regulatory_audit_query(table, "2026-05-10")
except ValueError as e:
    print(f"Audit broken: {e}")
```

**Lesson:** tag deletion is an audit-relevant event. In production:
- Restrict who can delete tags (RBAC)
- Log tag operations to immutable audit storage
- Set up alerts on tag deletion

### Break 2: PII in metadata

Imagine `txn_id` were a user's national ID number instead of a synthetic ID. **The min/max bounds in manifests would contain PII.** Schema evolution wouldn't fix this — old manifests retain bounds.

**Lesson:** never put identifying values in primary keys / partition keys. Use synthetic IDs. This is a real production issue.

### Break 3: cross-jurisdiction data residency

If your fintech operates in DE and NL, and data must stay in EU:
- MinIO/S3 bucket must be in EU region
- The catalog database must be in EU region
- Any compute (Spark, Trino) must be in EU region
- Any third-party services (e.g., monitoring, observability) must be in EU region or DPA-covered

**One link being non-EU = compliance breach.** Iceberg is data-residency-friendly (all artifacts are in your storage), but the surrounding architecture must match.

---

## 📚 Theory deep-dive

### The regulatory mapping

| Regulation | Requirement | Iceberg mechanism |
|---|---|---|
| **DORA Art. 6** | ICT risk management | Snapshot isolation prevents partial-write corruption |
| **DORA Art. 9** | Data integrity, authenticity, traceability | Snapshot summaries, manifest checksums, append-only history |
| **DORA Art. 28** | ICT third-party risk, data portability | Open spec, multi-engine readable, no vendor lock-in |
| **BaFin BAIT** | Strong access controls | Catalog-level RBAC (Lakekeeper, Glue, Polaris) |
| **MiFID II RTS 22** | Transaction reporting fields | Schema with documented field-ids + table property tagging |
| **MiFID II RTS 25** | Reproducibility of reports | EOD tag pattern + time travel |
| **MiFID II RTS 25** | Microsecond timestamp precision | Iceberg TimestampType supports microseconds |
| **GDPR Art. 5(1)(e)** | Storage limitation | Snapshot expiration with documented policy |
| **GDPR Art. 17** | Right to erasure | Delete + redaction workflow + audit log |
| **GDPR Art. 30** | Records of processing | Table properties as documentation layer |
| **EU AI Act** | High-risk system data quality | Lineage via snapshot summaries; field-level documentation |

This table is gold for interviews. Reproduce it from memory.

### Where Iceberg fits versus what you still need

Iceberg gives you the **technical foundation** for compliance. It does NOT give you:

- Access control (need a catalog or policy engine like OPA)
- Data classification (need a data catalog like OpenMetadata)
- PII detection (need scanning tools)
- Lineage across systems (need OpenLineage / Marquez)
- Audit reporting (need a BI layer)
- Encryption at rest (need S3 SSE or KMS)

**A complete compliance architecture has many components.** Iceberg is the data-format-level piece. Knowing where Iceberg's responsibility ends and other tools begin = senior signal.

### The "regulatory metadata layer" pattern

Treat Iceberg table properties as a structured regulatory annotation system:

```python
properties = {
    "regulatory.classification": "MiFID-II-RTS-22",
    "regulatory.retention_min_years": "7",
    "regulatory.contains_pii": "true",
    "regulatory.lawful_basis": "Art.6(1)(b)-contract",
    "regulatory.data_residency": "EU",
    "regulatory.owner_team": "trading-data-platform",
    "regulatory.last_dpia_date": "2026-04-15",
}
```

Build tools that query catalog metadata for governance:

```sql
-- "Show me all tables containing PII without a recent DPIA"
SELECT table_name FROM iceberg_tables
WHERE properties['regulatory.contains_pii'] = 'true'
  AND properties['regulatory.last_dpia_date'] < DATE '2026-01-01';
```

**This is the kind of platform thinking that gets you hired into senior platform roles.**

### The audit reproducibility pattern (the "regulatory crown jewel")

```
Daily ETL writes
        ↓
End-of-day quality check
        ↓
Pass? → Create EOD tag → Tag persists for 7+ years
        ↓
Run regulatory reports off the EOD tag
        ↓
Snapshots between EOD tags can expire after default window
        ↓
Auditor query 5 years later → time-travel to EOD tag → exact reproduction
```

This pattern is **the** answer to MiFID II RTS 25. Practice describing it in 60 seconds.

---

## 🇪🇺 Bonus: positioning yourself in interviews

This entire experiment is your differentiation. Most candidates can describe Iceberg's technical features. You can describe **Iceberg as a compliance architecture choice**. The frames you should practice using:

> "Iceberg is the most defensible lakehouse format for an EU regulated environment, for three reasons: vendor-neutral governance (DORA Art. 28), exact reproducibility via tags (MiFID II RTS 25), and inspectability of the underlying spec (BaFin auditability)."

> "We use end-of-day snapshot tags as our regulatory baseline. Any auditor question of the form 'what was the state at date X' becomes a one-line query."

> "GDPR is the one place where Iceberg's strengths cut against compliance. We mitigate via a redaction workflow with documented SLA and an audit log of deletion events."

> "Our table properties carry the regulatory metadata. We can answer governance questions across thousands of tables by querying the catalog."

**Practice these out loud.** Record yourself. Adjust until they sound natural. **You will be asked questions that map onto these answers.**

---

## ✍️ Re-answer the interview question

> "How does Iceberg help with regulatory compliance? Walk me through a regulated lakehouse design."

Cover:

1. The regulatory mapping table (DORA, MiFID II, GDPR → Iceberg mechanisms)
2. EOD tagging pattern for reproducibility
3. Regulatory metadata via table properties
4. The GDPR / time-travel tension and how to resolve it
5. What Iceberg doesn't provide (access control, classification, lineage) and what fills those gaps

This is a 3–5 minute answer in an interview. **Practice it.** Length isn't a flaw here — it shows you know the topic in depth.

---

## 🎁 LinkedIn post draft

> **"I designed an Iceberg lakehouse for an EU bank. Here's the regulatory checklist I wish existed when I started."**
>
> Use the regulatory mapping table. This is THE post that establishes your name in EU FinTech data engineering. Tag relevant German FinTech accounts. Repost in 2 weeks with a "what people are asking" follow-up.

**Strategic note:** this post is also a marketing channel for the future DORA-compliance SaaS you've been exploring. Readers who engage = potential customers. Treat the comments as customer discovery interviews.

---

## Session wrap-up (close the loop)

1. Confirm hands-on is done: compliance-oriented table design, snapshot/tag or audit pattern exercised, regulatory mapping understood, **after** re-answer written.
2. Update `interview-faq.md` with your **after** answer to this experiment's interview question (and review the full 33-question list in [`experiments/README.md`](README.md)).
3. If Break it left the lab in a broken state: `cd lab && ./reset.sh --confirm` — see [operation guide §7](../docs/operation-guide.md).
4. **End of day** (pause until tomorrow, keep data): `cd lab && docker compose stop` — [operation guide §8](../docs/operation-guide.md).
5. **Finished the entire series on this machine:** [operation guide §10](../docs/operation-guide.md) (full wipe), then follow **What to do next** below to publish and distribute.

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md).

---

## You've completed the series ✅

If you've done all 13 experiments hands-on AND written your answers in `interview-faq.md`, you should now be able to:

- Answer all 30 interview questions from the index without notes
- Articulate Iceberg's architecture in any depth from 30 seconds to 30 minutes
- Defend architectural decisions in a way that demonstrates both technical and regulatory awareness
- Lead a senior-level technical interview confidently

This puts you in the top 5–10% of candidates for data platform / data engineering roles in EU FinTech.

---

## What to do next

1. **Publish.** Push the repo to GitHub. Write the README so it's discoverable. Post the first LinkedIn piece this week.
2. **Tag a few targeted engineers** — DBG, Trade Republic, Commerzbank Digital data leads. Not spam-tagging; meaningful tagging where the content is genuinely relevant to them.
3. **Use the interview-faq.md as your spaced-repetition deck.** Re-read it every 3 days for 2 weeks. After that, every 2 weeks until you've interviewed.
4. **Update SLH's ADRs.** This work should generate at least one new ADR (`ADR-015 Iceberg as compliance-foundation format`).
5. **Plan the next series.** What comes after Iceberg mastery? Trino-on-Iceberg? OpenMetadata for governance? The community has signaled interest by reading this series. Listen.

You built a real asset. **Now distribute it.**
