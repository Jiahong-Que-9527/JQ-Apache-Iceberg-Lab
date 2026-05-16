# Experiment 08 — Concurrent Writes & Isolation

> **Time:** 90 min · **Tier:** Mechanics · **Prerequisites:** Experiments 01–07

---

## 🎯 The interview question this answers

> **"Two writers commit to the same table at the same time. What happens? Walk me through Iceberg's optimistic concurrency control."**

Write your current answer in `interview-faq.md`.

**Why this matters:** Concurrent write behavior is where lakehouses get scary in production. Most candidates can't articulate what isolation level Iceberg provides, when conflicts occur, or how retry works. Being precise here = senior signal.

---

## TL;DR

Iceberg uses **optimistic concurrency control (OCC)**. Writers do all their work optimistically, then race to update the catalog pointer. The loser detects the conflict (because the parent snapshot moved), re-validates whether its changes are still safe, and retries. The isolation level is **snapshot isolation**, not serializable — which matters for some workloads.

---

## 🛠️ Hands-on: cause and resolve conflicts

### Step 1 — Set up

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType
import pyarrow as pa

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "id", StringType(), required=True),
    NestedField(2, "value", LongType()),
)
table = catalog.create_table("lab.concurrent", schema=schema)
table.append(pa.table({"id": ["initial"], "value": [0]}))
```

### Step 2 — Simulate two writers (manual conflict)

This is best done in two notebook kernels OR two terminals running Python REPLs.

**Terminal A:**

```python
from src.catalog_helper import get_catalog
import pyarrow as pa

catalog = get_catalog()
table_a = catalog.load_table("lab.concurrent")
print("A: loaded table, current snapshot:", table_a.current_snapshot().snapshot_id)
```

**Terminal B:**

```python
from src.catalog_helper import get_catalog
import pyarrow as pa

catalog = get_catalog()
table_b = catalog.load_table("lab.concurrent")
print("B: loaded table, current snapshot:", table_b.current_snapshot().snapshot_id)
```

Both A and B should print the same snapshot ID.

**Terminal A — commits first:**

```python
table_a.append(pa.table({"id": ["from_A"], "value": [100]}))
print("A: committed, new snapshot:", table_a.current_snapshot().snapshot_id)
```

**Terminal B — commits without refreshing:**

```python
try:
    table_b.append(pa.table({"id": ["from_B"], "value": [200]}))
    print("B: committed without conflict!")
except Exception as e:
    print(f"B: conflict! {type(e).__name__}: {e}")
```

What you see depends on the PyIceberg version. **In most current versions, PyIceberg automatically retries on conflict**, so B succeeds — but the underlying mechanism is: B detected the parent had moved, re-fetched the table state, and retried.

To force a visible conflict, use the lower-level transaction API:

```python
from pyiceberg.transactions import Transaction

# Terminal B, explicit transaction
table_b = catalog.load_table("lab.concurrent")
with table_b.transaction() as txn:
    txn.append(pa.table({"id": ["from_B_explicit"], "value": [201]}))
    # If A commits during the with-block, you may see CommitFailedException
```

The cleanest demo: use a debugger to pause B between "compute changes" and "commit" while A commits.

### Step 3 — Verify both writes eventually land

```python
table = catalog.load_table("lab.concurrent")
print(table.scan().to_arrow().to_pandas())
```

You should see all three rows: `initial`, `from_A`, `from_B`. Both writes succeeded — one via the original commit, one via retry.

Look at the snapshot history:

```python
for s in table.snapshots():
    print(f"{s.snapshot_id} parent={s.parent_snapshot_id} op={s.summary.get('operation')}")
```

You'll see a linear history: initial → A's commit → B's commit (after retry). B's snapshot's parent is A's snapshot, even though B started before A committed.

### Step 4 — A conflict that CANNOT be resolved by retry

Some operations cannot retry safely. Example: **deleting rows** that depend on what currently exists.

```python
# Setup: a table with some rows
table = catalog.load_table("lab.concurrent")

# Both A and B want to delete rows where value < 50
# A succeeds. B's "delete by predicate" needs to re-check — does the row still exist?
# If it does, retry. If not, B's operation is a no-op (which is correct).

# But: if A and B are both modifying the SAME row (delete vs update), it's a real conflict.
```

PyIceberg handles delete-by-predicate as a special case that re-evaluates after retry. **The interesting case is updates on the same row** — covered by merge-on-read in v2 spec.

### Step 5 — Snapshot isolation in action (read during write)

Open a long-running read in one terminal:

```python
# Terminal A — start a scan
table_a = catalog.load_table("lab.concurrent")
scan_iter = iter(table_a.scan().to_arrow_batch_reader())  # streaming
print("A: started scan at snapshot", table_a.current_snapshot().snapshot_id)
first_batch = next(scan_iter)
print("A: read first batch")
```

In the meantime, in Terminal B:

```python
# Terminal B — commit a write
table_b = catalog.load_table("lab.concurrent")
table_b.append(pa.table({"id": ["written_during_A_scan"], "value": [999]}))
print("B: committed during A's scan")
```

Back in Terminal A:

```python
# Continue iterating
try:
    while True:
        batch = next(scan_iter)
        print(f"A: another batch of size {len(batch)}")
except StopIteration:
    pass

# A's scan should NOT see B's write — A is pinned to the pre-write snapshot.
print("A: final pinned snapshot was", table_a.current_snapshot().snapshot_id)
```

**A does not see B's data.** This is snapshot isolation. A's view of the world was fixed when its scan started.

### Step 6 — Branches for safe concurrent experiments

```python
# Create a branch
table = catalog.load_table("lab.concurrent")
current_snapshot_id = table.current_snapshot().snapshot_id
table.manage_snapshots().create_branch(
    snapshot_id=current_snapshot_id,
    branch_name="experiment_branch"
).commit()

table = catalog.load_table("lab.concurrent")
print("Branches:", list(table.refs.keys()))
```

Writes to a non-main branch don't conflict with writes to main. This is the **write-audit-publish pattern foundation**. (PyIceberg's branch-write support is improving — Spark/Java has the most mature support today.)

---

## 💥 Break it

### Break 1: many concurrent writers

Open 5 terminals. In each:

```python
from src.catalog_helper import get_catalog
import pyarrow as pa
import time, random

catalog = get_catalog()

for i in range(20):
    table = catalog.load_table("lab.concurrent")
    table.append(pa.table({"id": [f"writer_X_iter_{i}"], "value": [i]}))
    time.sleep(random.uniform(0, 0.5))
```

(Change `X` to a unique letter per terminal.)

Watch for `CommitFailedException` errors. Most retries will succeed. Some operations may fail permanently if retry exhaustion is hit (default retry count is limited).

**Insight:** OCC works well when conflicts are rare. As contention increases, retries multiply, and throughput drops. At extreme contention, you need to redesign the architecture (e.g., partition writes, use a queue, batch).

### Break 2: simulate retry exhaustion

```python
# This requires lower-level transaction control
# See iceberg-python docs for retry configuration

# Conceptually: set max_retries=1, then race many writers
# Many will fail with CommitFailedException
```

In production, **monitor commit retry rates**. High retries = a hot table that needs architectural review.

### Break 3: cross-table consistency

Iceberg's transactions are **per-table**, not cross-table. Try:

```python
table_a = catalog.create_table("lab.tableA", schema=schema)
table_b = catalog.create_table("lab.tableB", schema=schema)

# Write to A
table_a.append(pa.table({"id": ["a"], "value": [1]}))
# Crash here, before writing to B
# Result: A has data, B doesn't. NO atomic across two tables.
```

**Iceberg does not provide cross-table atomic commits.** For that, you need Nessie (which supports multi-table transactions via Git-like commits) or a higher-level workflow tool.

This is a real limitation. Mention it in interviews when asked about Iceberg's tradeoffs — shows you know the edges.

---

## 📚 Theory deep-dive

### Optimistic vs. pessimistic concurrency

| | Pessimistic (locking) | Optimistic (OCC, Iceberg) |
|---|---|---|
| Mechanism | Lock the resource before working | Work first, validate at commit |
| Best when | Conflicts are common | Conflicts are rare |
| Worst when | Many readers (lock contention) | Many writers (retry storms) |
| Latency | High under contention | Low when uncontended |
| Implementation | Stateful coordinator | Stateless |

Iceberg chose OCC because **most lakehouse workloads have many readers and a few writers** (often just one ETL pipeline writing). Conflicts are rare. When they occur, retry usually resolves them.

### The commit protocol (step by step)

1. Writer reads current `metadata.json` location from the catalog
2. Writer reads the metadata, finds current snapshot's manifest list
3. Writer creates new Parquet data files in S3
4. Writer creates new manifest(s) listing the new files
5. Writer creates a new manifest list combining new + existing manifests
6. Writer creates a new `v{N+1}.metadata.json`
7. Writer asks catalog: "update pointer from v{N} to v{N+1}, but **only if the current pointer is still v{N}**"
8. If catalog says yes: commit success
9. If catalog says no (another writer updated it): **conflict**
   - Refresh state
   - Re-validate: can my changes still be applied to the new state?
   - If yes: rewrite manifest list (now pointing at the *newer* manifests as existing), retry from step 6
   - If no: fail with CommitFailedException

The retry logic distinguishes Iceberg from naive last-write-wins systems.

### When retry works vs. when it fails

**Retry succeeds (most common):**
- Both writers appended new data → both can land
- Both writers added different partitions → both can land
- One writer appended, one rewrote a different partition → both can land

**Retry fails:**
- Both writers tried to overwrite the same data
- Both writers tried to delete the same row (one succeeds, the other's "delete" is a no-op which may or may not be an error depending on the operation)
- Schema-level conflicts (both writers changed schema in incompatible ways)

### Isolation level: snapshot, not serializable

**Snapshot isolation** (Iceberg's level):
- A transaction reads from a fixed snapshot
- It cannot see writes that happened during the transaction
- It commits successfully if no other transaction modified the *files it touched*

**Serializable** (stronger):
- A transaction's effect must be equivalent to *some* serial ordering of all transactions
- Detects "phantoms" — rows that appeared during the transaction and would have affected the result

Most lakehouses provide snapshot isolation. **This is enough for 90% of workloads but NOT enough for:**
- "Read total, then write 10% increase" (the total may have changed during read)
- Constraint enforcement that depends on a global state

For these, you need application-level coordination or a different system.

### Production tuning

Things to think about:

- **Retry count and backoff** — defaults are reasonable; tune for your workload
- **Commit batching** — many small commits → many retry chances; batch where possible
- **Partition-based write routing** — if writers can be assigned non-overlapping partitions, conflicts vanish
- **Use branches for "staging"** — write to branch, validate, fast-forward main (avoids contention with online readers)

---

## 🇪🇺 Regulatory angle

**DORA Article 6** (ICT risk management framework) requires firms to manage operational integrity. Concurrent-write conflicts are an integrity risk if mishandled. Iceberg's snapshot isolation + atomic commit provides a defensible technical control:

- No partial writes are ever visible
- All commits are recorded in snapshot history (auditable)
- The retry mechanism is deterministic and observable

If asked "how do you handle concurrent writes to regulatory data?" — explain Iceberg's OCC + snapshot isolation + the audit trail in snapshot summaries. Add: "For cases where snapshot isolation isn't sufficient (e.g., balance enforcement), we route those writes through a coordinator service rather than relying on the table format alone." Shows you know where the format's guarantees end.

---

## ✍️ Re-answer the interview question

> "Two writers commit at the same time. What happens?"

Cover:

1. Optimistic concurrency control: writers work optimistically, race at commit
2. The conditional update on the catalog pointer is the atomic primitive
3. Conflict detection: parent snapshot moved → retry
4. Retry can succeed if changes are still applicable to new state (most cases)
5. Isolation level: snapshot isolation (not serializable — name the gap)
6. Operational implication: monitor retry rates, partition-route writes to reduce contention

---

## 🎁 LinkedIn post draft

> **"I ran 5 concurrent writers against an Iceberg table. Most retries succeeded. Here's what happened in the failures, and why."**
>
> Walk through OCC. End with: "Iceberg gives you snapshot isolation, not serializable. For most workloads, that's exactly right. Knowing when it isn't is the senior data engineer's job."

---

## Mechanics tier complete ✅

You've covered the mechanical guts of Iceberg: snapshots, schema evolution, partitioning, concurrent writes. Before moving to Tier 3:

- Re-answer interview questions 7–15 in your `interview-faq.md` without notes
- If any answer is fuzzy, redo that experiment

---

## Session wrap-up (close the loop)

1. Confirm hands-on is done: concurrent append / OCC behavior observed (success + `CommitFailedException` or equivalent), Break it completed.
2. Update `interview-faq.md` with your **after** answer to this experiment's interview question.
3. If Break it left the lab in a broken state: `cd lab && ./reset.sh --confirm` — see [operation guide §7](../docs/operation-guide.md).
4. **End of day** (pause until tomorrow, keep data): `cd lab && docker compose stop` — [operation guide §8](../docs/operation-guide.md).
5. **Done with the lab on this machine** (remove all local tables and catalogs): [operation guide §10](../docs/operation-guide.md).

Lab lifecycle overview: [operation guide §0](../docs/operation-guide.md).

---

## Next up

→ [Experiment 09: The small files problem](09-small-files-compaction.md) — every lakehouse's #1 production issue.
