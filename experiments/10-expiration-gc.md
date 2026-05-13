# Experiment 10 — Snapshot Expiration & Garbage Collection

> **Time:** 75 min · **Tier:** Production · **Prerequisites:** Experiments 01–09

---

## 🎯 The interview question this answers

> **"What does `expire_snapshots` do? What are its risks? How do you balance time travel against storage cost? How do you handle GDPR right-to-be-forgotten?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** Storage management is where lakehouses get expensive. Knowing how to expire snapshots safely — and the trade-off with time travel and compliance — separates "I've used Iceberg" from "I've operated Iceberg in production."

---

## TL;DR

Every snapshot keeps its data files alive. Without expiration, your storage grows forever even after deletes. `expire_snapshots` removes old snapshots from metadata; **then a separate "orphan file" cleanup** removes the actual data files that no remaining snapshot references. **The two operations are separate by design — for safety.** This is where GDPR right-to-be-forgotten gets complicated.

---

## 🛠️ Hands-on: see storage grow, then reclaim it

### Step 1 — Set up and create some history

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType
import pyarrow as pa
import boto3, os

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "id", StringType(), required=True),
    NestedField(2, "value", LongType()),
)
table = catalog.create_table("lab.gc_demo", schema=schema)

# Commit 1: write 1000 rows
table.append(pa.table({"id": [f"a{i}" for i in range(1000)], "value": list(range(1000))}))

# Commit 2: overwrite with different 1000 rows
table = catalog.load_table("lab.gc_demo")
table.overwrite(pa.table({"id": [f"b{i}" for i in range(1000)], "value": list(range(1000))}))

# Commit 3: overwrite again
table = catalog.load_table("lab.gc_demo")
table.overwrite(pa.table({"id": [f"c{i}" for i in range(1000)], "value": list(range(1000))}))

table = catalog.load_table("lab.gc_demo")
```

### Step 2 — Measure storage

```python
s3 = boto3.client("s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

# List everything in the table's S3 prefix
def total_storage(table_location):
    bucket = "warehouse"
    prefix = table_location.replace(f"s3://{bucket}/", "")
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total += obj["Size"]
            count += 1
    return total, count

total, count = total_storage(table.location())
print(f"Total storage: {total/1024:.1f} KB across {count} files")
```

You'll see all three commits' data files still present, even though the **current** table only has the third commit's data.

### Step 3 — Try to read an old snapshot

```python
snapshots = list(table.snapshots())
first_snapshot = snapshots[0]

result = table.scan(snapshot_id=first_snapshot.snapshot_id).to_arrow()
print(f"Snapshot 1 still readable: {len(result)} rows, first id: {result['id'][0]}")
```

Time travel works because the old data files are still there.

### Step 4 — Expire old snapshots

```python
import time

# Keep only the current snapshot — expire everything older than "now"
# (in production you'd set a sensible retention window, e.g., 7 days)

current_time_ms = int(time.time() * 1000)

table.expire_snapshots().expire_older_than(current_time_ms - 1000).commit()  # 1 second ago
table = catalog.load_table("lab.gc_demo")

print(f"Snapshots after expiration: {len(list(table.snapshots()))}")
```

Now try to read the old snapshot:

```python
try:
    table.scan(snapshot_id=first_snapshot.snapshot_id).to_arrow()
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

Time travel to expired snapshots **fails** — the metadata says it doesn't exist anymore.

### Step 5 — But the data files might still be there!

```python
total, count = total_storage(table.location())
print(f"Storage AFTER expiration: {total/1024:.1f} KB across {count} files")
```

You might see the storage hasn't dropped, or only dropped a little.

**This is the critical insight:** `expire_snapshots` only removes snapshots from the table's metadata. **It does not automatically delete the orphaned data files.** You need a separate cleanup operation.

### Step 6 — Remove orphan files

PyIceberg's API surface for this varies; the canonical operation is called `remove_orphan_files`:

```python
# Conceptual — in production you'd typically run this from Spark or as a scheduled action
# Pseudo-code (check pyiceberg actions for the exact current API):

# from pyiceberg.table.actions import RemoveOrphanFiles
# RemoveOrphanFiles(table).execute(older_than_ms=...)

# Manual approach for demo:
# 1. Get all data file paths currently referenced by valid snapshots
# 2. List all files in S3 under the table location
# 3. Find files in S3 not referenced by any snapshot
# 4. Delete them (with a safety threshold for files newer than X)

# This is conceptually what the action does internally.
```

The reason orphan file cleanup is separate:

- **Safety.** If a write is in progress (data files created but not yet committed), those files exist in S3 but aren't yet referenced. Aggressive cleanup would delete them mid-write.
- **Tooling differences.** Different engines may have created files; cleanup must understand all of them.
- **Cost control.** Orphan cleanup involves LIST operations on S3 (expensive at scale); scheduling it separately lets you control cost.

**In production:** schedule `expire_snapshots` daily, `remove_orphan_files` weekly with a safety window (e.g., only delete files older than 24h).

### Step 7 — See what tag/branch refs protect

Refs (tags and branches) **protect** snapshots from expiration. Tag a snapshot, then try to expire it:

```python
# Create more commits to have something old
table = catalog.load_table("lab.gc_demo")
for i in range(3):
    table.append(pa.table({"id": [f"new_{i}"], "value": [i]}))
    table = catalog.load_table("lab.gc_demo")

# Get the oldest snapshot
snapshots = list(table.snapshots())
oldest = snapshots[0]

# Tag it as "regulatory-2026-Q1"
table.manage_snapshots().create_tag(
    snapshot_id=oldest.snapshot_id,
    tag_name="regulatory-2026-Q1"
).commit()
table = catalog.load_table("lab.gc_demo")

# Now try to expire — the tagged snapshot should survive
import time
table.expire_snapshots().expire_older_than(int(time.time() * 1000)).commit()
table = catalog.load_table("lab.gc_demo")

snapshot_ids = [s.snapshot_id for s in table.snapshots()]
print(f"Snapshot {oldest.snapshot_id} still present: {oldest.snapshot_id in snapshot_ids}")
print(f"Current refs: {dict(table.refs)}")
```

The tagged snapshot survives. **This is how you implement regulatory retention.** Tag the snapshots you must keep; let everything else age out.

---

## 💥 Break it

### Break 1: GDPR right-to-be-forgotten

GDPR says: when a user requests deletion, you must remove their data. Iceberg's snapshot history works against this if you keep snapshots forever.

```python
# User requests deletion. You delete their rows in the current snapshot:
table.delete(delete_filter="user_id = 12345")  # or equivalent

# But: their data is still in OLD snapshots!
# To truly comply, you must:
# 1. Delete the rows in the current snapshot (done)
# 2. Expire all snapshots that contain the user's data
# 3. Run orphan file cleanup to remove the actual files
# 4. Document the process for the audit trail
```

The hard part: **you cannot selectively expire only "snapshots containing user X."** You have to expire by time. So GDPR compliance requires keeping a maximum snapshot retention window that's compatible with the deletion SLA (typically 30 days from request).

**This is one of the rare cases where regulatory needs and Iceberg's strengths conflict.** Acknowledging this in interviews shows real maturity.

### Break 2: aggressive expiration breaks readers mid-query

A long-running query has pinned an old snapshot. You expire it mid-query.

```python
# Conceptually:
# Reader: started scan at snapshot 100, reading files...
# Admin: expires snapshot 100, removes orphan files
# Reader: tries to open the next file → 404
```

**Mitigation:** set a safety window. Don't expire snapshots younger than your longest-running query. Don't remove orphan files younger than your safety threshold (often 24-72h).

### Break 3: re-add a deleted tag

```python
# Drop the tag
table.manage_snapshots().remove_tag("regulatory-2026-Q1").commit()
table = catalog.load_table("lab.gc_demo")

# Now expire — the previously-tagged snapshot is unprotected
table.expire_snapshots().expire_older_than(int(time.time() * 1000)).commit()
```

**Lesson:** dropping a tag is a high-stakes operation. In production, audit tag changes.

---

## 📚 Theory deep-dive

### Why expiration is two phases

Phase 1: **Logical expiration** (`expire_snapshots`).
- Removes snapshots from `metadata.json`
- Updates references; old snapshots no longer reachable
- Fast, atomic, fully recoverable until the next phase runs

Phase 2: **Physical cleanup** (`remove_orphan_files`).
- Lists all files in S3 under the table location
- Compares against all files referenced by remaining snapshots
- Deletes files referenced by no snapshot AND older than a safety threshold

The two-phase design lets you recover from mistakes. If you accidentally expire a snapshot, you have a window before its data files are physically deleted to recreate it. This is a feature, not a bug.

### Retention policy framework

A production retention policy has multiple dimensions:

1. **Default retention window** — how far back can anyone time-travel? (e.g., 7 days)
2. **Tagged retention** — specific snapshots kept indefinitely (regulatory baselines)
3. **Branch retention** — branches are kept until explicitly dropped
4. **Min snapshots** — always keep at least N snapshots regardless of age (safety net)
5. **Orphan file safety window** — don't delete files newer than X (e.g., 72h)

Combine all five. Example policy:

> "Default 7-day retention. Tags for month-end snapshots kept 7 years. Daily expiration job. Weekly orphan file cleanup with 72h safety window. Min 10 snapshots always retained."

In an interview, articulating a policy at this level of specificity = you've done this.

### Storage cost model

For a streaming table that grows at 100GB/day and gets compacted hourly:

- Without expiration: storage grows linearly forever
- With 7-day expiration: storage stabilizes at ~700GB + the size of the current snapshot's data
- With 30-day expiration + tags: storage grows but predictably

**Rough math at AWS S3 Standard pricing:** $23/TB/month. Over a year, the difference between 7-day and 30-day retention on a 100GB/day table is ~$50/month. Worth it? Depends on the value of time travel.

### The branch-write-audit-publish pattern, revisited

WAP makes expiration safer:

```
1. Spark writes new data to branch "wap"
2. Run quality checks
3. If passes: fast-forward main → wap. Drop branch wap.
4. If fails: drop branch wap. Run remove_orphan_files. Bad data never visible.
```

Without WAP, you might write bad data to main, then have to roll back, leaving orphans. WAP cleanup is cleaner.

---

## 🇪🇺 Regulatory angle

This experiment is where the EU regulatory story gets nuanced.

**MiFID II RTS 25** — keep audit trails for 5 years. Solution: tag end-of-day snapshots, set tag retention to 7 years (with buffer), default expiration to 30 days. Old tags survive; non-tagged history rotates.

**DORA Article 28** — ICT third-party risk, including data portability. Compatible with Iceberg if expiration policies are clearly documented and predictable.

**GDPR Article 17** — right to erasure. This is where it gets hard:
- A user requests deletion
- You must delete their data within 30 days (typically)
- Iceberg snapshot history retains the data until expiration + orphan cleanup
- Therefore: your max snapshot retention for tables with personal data must be < 30 days, OR you must implement a "redaction" workflow

**The redaction workflow** (more common in practice):
- Maintain a "redaction log" of GDPR deletion requests
- Run a special compaction job that rewrites affected partitions, removing the user's rows
- Expire the snapshots that contained the user's data
- Document the process; auditors will check this

This is the kind of detail that gets you hired at EU banks. **Most candidates have not thought about it.**

---

## ✍️ Re-answer the interview question

> "What does `expire_snapshots` do? What are its risks? How do you handle GDPR?"

Cover:

1. Two-phase model: logical expiration vs. physical orphan file cleanup
2. Tags/branches protect snapshots from expiration — used for regulatory retention
3. Safety windows: don't expire snapshots in active use, don't delete files mid-write
4. GDPR conflict: snapshot history vs. right-to-erasure. Resolved via short retention or redaction workflow
5. Production policy: default window + tagged exceptions + orphan cleanup with safety threshold
6. Cost tradeoff: storage cost vs. time travel value

---

## 🎁 LinkedIn post draft

> **"Iceberg's time travel is a superpower — until GDPR shows up. Here's the retention policy I use to balance them."**
>
> Real, specific, EU-relevant. End with the redaction workflow. This will get attention from EU FinTech engineers who are wrestling with the same issue.

---

## Next up

→ [Experiment 11: Iceberg vs Delta vs Hudi](11-iceberg-vs-delta-vs-hudi.md) — the comparison every interviewer will ask about.
