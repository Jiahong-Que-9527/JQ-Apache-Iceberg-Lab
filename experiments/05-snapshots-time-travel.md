# Experiment 05 — Snapshots & Time Travel

> **Time:** 90 min · **Tier:** Mechanics · **Prerequisites:** Tier 1 complete

---

## 🎯 The interview question this answers

> **"How does Iceberg implement ACID transactions? What is a snapshot, and how does time travel work?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** This question separates people who think "Iceberg is just versioned Parquet" from people who understand the snapshot-based MVCC model. The latter answer is what hiring managers want to hear.

---

## TL;DR

A snapshot is **an immutable record of the table's state at a point in time**, represented as a manifest list. Iceberg never overwrites snapshots — every write creates a new one. Time travel is "just" reading any past snapshot's manifest list instead of the current one. ACID falls out of the same mechanism: readers see a consistent snapshot, writers race to update the catalog pointer.

---

## 🛠️ Hands-on: build the snapshot history

### Step 1 — Three commits, three snapshots

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, DoubleType
import pyarrow as pa

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "id", StringType(), required=True),
    NestedField(2, "amount", DoubleType()),
    NestedField(3, "label", StringType()),
)
table = catalog.create_table("lab.history", schema=schema)

# Commit 1
table.append(pa.table({
    "id": ["a", "b"], "amount": [10.0, 20.0], "label": ["v1", "v1"],
}))

# Commit 2
table = catalog.load_table("lab.history")
table.append(pa.table({
    "id": ["c"], "amount": [30.0], "label": ["v2"],
}))

# Commit 3 — overwrite
table = catalog.load_table("lab.history")
table.overwrite(pa.table({
    "id": ["x", "y"], "amount": [100.0, 200.0], "label": ["v3", "v3"],
}))

table = catalog.load_table("lab.history")
```

### Step 2 — Inspect the snapshot history

```python
for snap in table.snapshots():
    print(f"Snapshot ID: {snap.snapshot_id}")
    print(f"  Parent:    {snap.parent_snapshot_id}")
    print(f"  Timestamp: {snap.timestamp_ms}")
    print(f"  Operation: {snap.summary.get('operation')}")
    print(f"  Summary:   {dict(snap.summary)}")
    print()
```

You should see three snapshots. **Each one has a parent pointer back to the previous.** This forms a linked list — the snapshot DAG.

Also try:

```python
# Using pyiceberg's inspection API
print(table.inspect.snapshots().to_pandas())
```

### Step 3 — Time travel: read past snapshots

```python
snapshots = list(table.snapshots())

# Read the FIRST snapshot
first = snapshots[0]
print(f"\n--- As of snapshot {first.snapshot_id} (commit 1) ---")
print(table.scan(snapshot_id=first.snapshot_id).to_arrow().to_pandas())

# Read the SECOND snapshot
second = snapshots[1]
print(f"\n--- As of snapshot {second.snapshot_id} (commit 2) ---")
print(table.scan(snapshot_id=second.snapshot_id).to_arrow().to_pandas())

# Read the CURRENT (third)
print(f"\n--- Current ---")
print(table.scan().to_arrow().to_pandas())
```

Watch carefully:
- Snapshot 1: 2 rows (a, b)
- Snapshot 2: 3 rows (a, b, c) — append preserved old data
- Snapshot 3: 2 rows (x, y) — overwrite replaced everything

**The old data files still exist on disk.** Verify in MinIO console — the parquet files from commit 1 are still there. Iceberg just stopped pointing at them in the current snapshot.

### Step 4 — Time travel by timestamp

```python
import time

# Get the timestamp of snapshot 2
target_ts = second.timestamp_ms

# pyiceberg supports timestamp travel — check API:
# In newer versions: table.scan(as_of=target_ts/1000)
# Or: find the snapshot whose timestamp <= target_ts

# Manual approach (always works):
matching = [s for s in table.snapshots() if s.timestamp_ms <= target_ts]
matching.sort(key=lambda s: s.timestamp_ms)
target_snap = matching[-1]

print(f"State as of {target_ts}:")
print(table.scan(snapshot_id=target_snap.snapshot_id).to_arrow().to_pandas())
```

### Step 5 — Rollback

You shipped bad data in commit 3. Rollback to commit 2:

```python
table.manage_snapshots().rollback_to_snapshot(second.snapshot_id).commit()
table = catalog.load_table("lab.history")
print("After rollback:")
print(table.scan().to_arrow().to_pandas())
```

You should see 3 rows again (a, b, c). **But check the snapshot history:**

```python
for snap in table.snapshots():
    print(f"{snap.snapshot_id} — {snap.summary.get('operation')}")
```

**Commit 3 is still in the history.** Rollback didn't delete it — it created a new "current" pointer. This is critical. You can roll *forward* again by rolling back to commit 3's snapshot_id.

### Step 6 — Branches and tags

Iceberg supports Git-like refs. Tag the current state:

```python
table.manage_snapshots().create_tag(
    snapshot_id=table.current_snapshot().snapshot_id,
    tag_name="known-good-2026-05-13"
).commit()

table = catalog.load_table("lab.history")
print(table.refs)
```

Create an experimental branch:

```python
table.manage_snapshots().create_branch(
    snapshot_id=table.current_snapshot().snapshot_id,
    branch_name="experiment"
).commit()
table = catalog.load_table("lab.history")
print(table.refs)
```

Now you can write to `experiment` without affecting `main`. (PyIceberg branch-write API is still evolving in some versions; in production you'd commonly do branch writes from Spark or via the REST catalog directly. For now, just understand the mental model.)

---

## 💥 Break it

### Break 1: try to read a snapshot that doesn't exist

```python
try:
    table.scan(snapshot_id=9999999999).to_arrow()
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

Clean error. Iceberg validates snapshot_id against the metadata before issuing reads.

### Break 2: delete a data file referenced by an old snapshot

In MinIO, manually delete one of the Parquet files from the first commit. Then:

```python
# Current snapshot doesn't reference it — should still work
print(table.scan().to_arrow().to_pandas())

# But old snapshot references it
try:
    table.scan(snapshot_id=first.snapshot_id).to_arrow()
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

**Insight:** time travel is only as durable as the data files. If you `expire_snapshots` (Experiment 10) and old files get cleaned up, time travel to those points becomes impossible. **You cannot have both aggressive GC and deep time travel.** This is a fundamental trade-off — covered in Experiment 10.

Reset before continuing.

### Break 3: concurrent commits and the snapshot DAG

In two notebook kernels simultaneously:

```python
# Kernel A
table = catalog.load_table("lab.history")
table.append(pa.table({"id": ["A1"], "amount": [1.0], "label": ["A"]}))
```

```python
# Kernel B
table = catalog.load_table("lab.history")
table.append(pa.table({"id": ["B1"], "amount": [2.0], "label": ["B"]}))
```

One succeeds. The other gets `CommitFailedException` because both tried to update the same parent pointer.

**Important:** the *retry behavior* matters here. PyIceberg's default `append` retries on conflict. So in practice, both may eventually succeed, with B's write becoming a child of A's snapshot. We explore this in depth in Experiment 08.

---

## 📚 Theory deep-dive

### The MVCC model

Iceberg implements **multi-version concurrency control (MVCC)**:

- Every write creates a new immutable snapshot
- Readers always see a single, complete snapshot — never a mix
- Snapshots form a DAG via parent pointers
- The catalog holds a single ref ("main") pointing to the current snapshot

This is structurally similar to Git:

| Git | Iceberg |
|---|---|
| Commit | Snapshot |
| Parent commit | parent_snapshot_id |
| HEAD | "main" branch ref |
| Branch | Iceberg branch |
| Tag | Iceberg tag |
| Detached HEAD read | Read by snapshot_id |
| `git reset --hard` | `rollback_to_snapshot` |
| `git gc` | `expire_snapshots` |

**Use this analogy in interviews.** It instantly conveys understanding.

### How ACID emerges from snapshots

| ACID property | How Iceberg achieves it |
|---|---|
| **Atomicity** | A commit either updates the catalog pointer (and is visible) or doesn't (and is invisible). No partial visibility. |
| **Consistency** | Schema validation happens at commit time. Constraints in the metadata are enforced. |
| **Isolation** | Readers pin to a snapshot at scan start. Concurrent writes create new snapshots — readers don't see them mid-scan. **Snapshot isolation level.** |
| **Durability** | All metadata and data files are written to durable storage (S3) before the catalog pointer updates. |

Note: the isolation level is **snapshot isolation**, not serializable. This matters for some workloads (e.g., concurrent UPDATE that depends on a read). See Experiment 08.

### Time travel — the limits

Iceberg time travel is **only** for reading. You cannot:

- Time-travel a write (write as if you were in the past)
- Compare two snapshots in a single query (need to do two scans and diff externally)
- Time-travel beyond what `expire_snapshots` has retained

The first two are sometimes available via add-on tools (Nessie supports more Git-like operations).

### When you'd use time travel in real production

1. **Debugging:** "the dashboard was wrong at 9am — what did the data look like then?"
2. **Reproducibility:** "train this ML model on the same data we used last quarter"
3. **Audit:** "regulator asks: what was the table state on 2026-03-31?"
4. **Rollback:** "the new ETL job corrupted the table — revert"
5. **Read-during-write isolation:** long-running analytics query keeps a consistent view while ingestion continues

**In interviews, give example #3 or #4.** They're the most enterprise-relevant.

### Branches and tags — the production patterns

- **Tag** = immutable reference to a specific snapshot. Use for: month-end snapshots, regulatory reporting baselines, "known-good" production state before risky changes.
- **Branch** = mutable reference that can have its own commits. Use for: write-audit-publish pattern, A/B testing schemas, experimental ETL pipelines.

The **write-audit-publish (WAP)** pattern is gold for interviews:

```
1. Spark writes new data to branch "wap"
2. Quality checks run against branch "wap"
3. If checks pass: fast-forward "main" to "wap" (atomic publish)
4. If checks fail: drop "wap", nothing visible to consumers
```

**This is how mature shops implement data quality without staging tables.** Worth knowing.

---

## 🇪🇺 Regulatory angle

The snapshot + tag model is **the single best technical answer to most data-related compliance questions**.

**MiFID II RTS 25** requires firms to reproduce reports as of any historical date. Implementation: tag the table at every report cutoff. Regulator asks for "March 31 close" → `table.scan(snapshot_id=tag("close-2026-03-31"))`. Done. Exact, defensible, audit-friendly.

**DORA Article 9** requires data integrity and traceability. Implementation: never `expire_snapshots` aggressively. Keep snapshots for the regulatory retention period. Every snapshot's summary records who wrote what and when (with proper write tooling).

**GDPR Article 17 (right to erasure)** is the *exception* — see Experiment 10 for the conflict between snapshot history and right-to-be-forgotten, and how it's typically resolved.

**In interviews at EU banks:** lead with "Iceberg's snapshot model maps very naturally onto MiFID II RTS 25 reproducibility requirements. We can tag the table at report cutoffs and the regulator can reconstruct any past state by-the-byte." This is the kind of cross-domain reasoning that few candidates demonstrate.

---

## ✍️ Re-answer the interview question

> "How does Iceberg implement ACID? What is a snapshot, and how does time travel work?"

Cover:

1. Snapshot = immutable record of table state, identified by snapshot_id, with a parent pointer
2. MVCC: every write creates a new snapshot, never modifies old ones
3. ACID: atomicity from atomic catalog pointer swap; isolation from snapshot pinning
4. Time travel: just read an older snapshot's manifest list
5. Branches/tags for production patterns (WAP, regulatory tagging)

Use the Git analogy. It works.

---

## 🎁 LinkedIn post draft

> **"Iceberg is to your data lake what Git is to your codebase. Here's the mapping table."**
>
> Use the Git/Iceberg comparison table from this experiment. End with: "If you understand `git reset --hard` you already understand `rollback_to_snapshot`."

The Git analogy goes viral every time someone rediscovers it. Be one of those people.

---

## Session wrap-up (close the loop)

1. Confirm hands-on is done: multiple snapshots listed, time travel read worked, old files still visible in MinIO. Optional notebook: `lab/notebooks/04_time_travel.ipynb`.
2. Update `interview-faq.md` with your **after** answer to this experiment's interview question.
3. If Break it left the lab in a broken state: `cd lab && ./reset.sh --confirm` — see [operation guide §7](../docs/operation-guide.md).
4. **End of day** (pause until tomorrow, keep data): `cd lab && docker compose stop` — [operation guide §8](../docs/operation-guide.md).
5. **Done with the lab on this machine** (remove all local tables and catalogs): [operation guide §10](../docs/operation-guide.md).

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md).

---

## Next up

→ [Experiment 06: Schema evolution](06-schema-evolution.md) — the field-id story. Why Iceberg can do what Parquet alone can't.
