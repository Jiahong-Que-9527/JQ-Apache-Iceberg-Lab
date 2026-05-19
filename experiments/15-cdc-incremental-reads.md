# Experiment 15 — CDC & Incremental Reads

> **Time:** 90 min · **Tier:** Production · **Prerequisites:** Experiments 01–10, 14 strongly recommended · **Spark profile: optional** (only for the "write side"; the consumer side is pure PyIceberg)

---

## 🎯 The interview question this answers

> **"How do you build a Change-Data-Capture consumer on top of an Iceberg table? Walk me through incremental reads, snapshot bookmarks, and the failure modes."**

Pause. Open `interview-faq.md`. Write your current answer.

**Why this matters:** In production, the writer is only half the story — the *consumer* is the other half. Every dbt incremental model, every Flink streaming job, every "sync Iceberg → search index" pipeline is built on incremental reads. Get the snapshot bookmark wrong and you either silently drop events or double-count them. Both are career-limiting bugs.

---

## TL;DR

Every Iceberg commit creates a new snapshot with a strictly increasing `snapshot_id` and a `parent_snapshot_id` forming a linear chain. A consumer remembers "last snapshot I processed" (a **bookmark**) and on next run asks Iceberg: "give me the rows added between bookmark and current." PyIceberg exposes this via `scan(snapshot_id=...)` for point-in-time reads and an **append-only incremental scan** that returns only data files added between two snapshots. **It does NOT, today, give you a full change-feed with row-level DELETE/UPDATE events** — that's the changelog scan, available in Spark via `system.changes` and still maturing in PyIceberg.

So in 2026: PyIceberg consumers handle **append-only** sources well, **CDC with deletes** still routes through a Spark changelog scan.

---

## 🛠️ Hands-on: build a working CDC consumer

### Step 1 — Reset, then create an append-only source table

```bash
cd lab && ./reset.sh --confirm
```

Open Jupyter at `http://localhost:8888`, create a notebook, paste:

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType
import pyarrow as pa
from datetime import datetime, timezone, timedelta

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=True),
    NestedField(3, "user_id", LongType()),
    NestedField(4, "amount",  LongType()),
)

if "lab.events_stream" in catalog.list_tables("lab"):
    catalog.drop_table("lab.events_stream")
table = catalog.create_table("lab.events_stream", schema=schema)
```

### Step 2 — Three commits = three snapshots

```python
def commit_batch(n, batch_id):
    now = datetime.now(timezone.utc)
    rows = pa.table({
        "event_id": [f"b{batch_id}_e{i}" for i in range(n)],
        "event_ts": [now + timedelta(seconds=i) for i in range(n)],
        "user_id":  [100 + (i % 5) for i in range(n)],
        "amount":   [(i + 1) * 10 for i in range(n)],
    })
    table.append(rows)

commit_batch(100, 1)
commit_batch(150, 2)
commit_batch(80,  3)

table = catalog.load_table("lab.events_stream")
for s in table.snapshots():
    print(f"id={s.snapshot_id}  parent={s.parent_snapshot_id}  op={s.summary.get('operation')}  added_rows={s.summary.get('added-records')}")
```

You should see three snapshots, parents chained, each with `operation=append`.

### Step 3 — The wrong consumer: scan the whole table every run

```python
def wrong_consumer():
    table = catalog.load_table("lab.events_stream")
    df = table.scan().to_arrow().to_pandas()
    return df

print("Wrong consumer rows:", len(wrong_consumer()))
```

This works on day 1. By day 365 with a 10 TB table, **this is a disaster** — you read everything every run. This is what 80% of teams ship first and rip out later.

### Step 4 — The right consumer: incremental scan with a bookmark

PyIceberg's `Table.scan` supports `snapshot_id=…` for point-in-time reads, and an `append_incremental` / `incremental_append_scan` API for "files added between two snapshots." API name varies by version — check `dir(table)` if needed. Below is the version-agnostic pattern using the snapshot chain directly:

```python
from pyiceberg.expressions import AlwaysTrue

class IcebergCDCConsumer:
    def __init__(self, table_identifier, bookmark_store):
        self.identifier = table_identifier
        self.bookmark_store = bookmark_store   # dict-like for the demo; Redis/PG in prod

    def run(self):
        table = catalog.load_table(self.identifier)
        latest = table.current_snapshot()
        if latest is None:
            return []  # empty table

        last_seen = self.bookmark_store.get(self.identifier)
        new_snapshots = self._snapshots_after(table, last_seen, latest.snapshot_id)

        new_files = []
        for snap in new_snapshots:
            if snap.summary.get("operation") != "append":
                raise RuntimeError(
                    f"snapshot {snap.snapshot_id} is operation={snap.summary.get('operation')}, "
                    f"this naive consumer only handles append-only sources"
                )
            for manifest in snap.manifests(table.io):
                for entry in manifest.fetch_manifest_entry(table.io):
                    if entry.snapshot_id == snap.snapshot_id and entry.status == 1:  # ADDED
                        new_files.append(entry.data_file)

        rows = self._read_files(table, new_files)
        self.bookmark_store[self.identifier] = latest.snapshot_id
        return rows

    @staticmethod
    def _snapshots_after(table, last_seen, current):
        # walk parent chain from current back to last_seen, then reverse
        chain = []
        snap = table.snapshot_by_id(current)
        while snap is not None and snap.snapshot_id != last_seen:
            chain.append(snap)
            if snap.parent_snapshot_id is None:
                break
            snap = table.snapshot_by_id(snap.parent_snapshot_id)
        return list(reversed(chain))

    @staticmethod
    def _read_files(table, data_files):
        # Each data_file gives us a Parquet path; we let PyArrow read them directly.
        import pyarrow.parquet as pq
        result = []
        fs = table.io
        for df in data_files:
            with fs.new_input(df.file_path).open() as fh:
                result.append(pq.read_table(fh))
        return pa.concat_tables(result) if result else pa.table({})

bookmarks = {}
consumer = IcebergCDCConsumer("lab.events_stream", bookmarks)

batch1 = consumer.run()
print("Run 1 new rows:", len(batch1), "bookmark =", bookmarks)
```

Run 1 picks up **all three** initial snapshots. Now commit more:

```python
commit_batch(20, 4)
commit_batch(35, 5)

batch2 = consumer.run()
print("Run 2 new rows:", len(batch2), "bookmark =", bookmarks)
```

Run 2 picks up only **55 new rows** (20 + 35) without re-reading the previous 330. This is the entire point.

### Step 5 — Idempotency: a re-run after a crash

Simulate the consumer crashing after reading the files but before committing the bookmark:

```python
# Simulate by not updating the bookmark
bookmarks_test = dict(bookmarks)  # snapshot the state
bookmarks_test["lab.events_stream"] = batch1[0].column_names and bookmarks["lab.events_stream"]  # unchanged

# Add new data
commit_batch(10, 6)

# Crash-recovery run with the OLD bookmark
old_bookmark = bookmarks["lab.events_stream"]
# Roll the bookmark back to before batch2 to simulate "didn't persist bookmark"
bookmarks["lab.events_stream"] = old_bookmark_before_batch2 = batch1_snapshot = batch1
```

In practice, the bookmark store is the source of truth. **Two correctness properties matter:**

1. **At-least-once**: the bookmark is updated *after* the downstream side-effect (DB write, message send) is durable.
2. **At-most-once**: the bookmark is updated *before* any side-effect, and the side-effect is idempotent on its own.

Most production CDC pipelines pick option (1) and require the downstream to be idempotent (UPSERT by `event_id`, MERGE INTO by primary key, etc.).

### Step 6 — Trying to consume a non-append commit

```python
# Force a non-append operation: overwrite
import pyarrow as pa
table = catalog.load_table("lab.events_stream")
replacement = pa.table({
    "event_id": ["replacement_1"],
    "event_ts": [datetime.now(timezone.utc)],
    "user_id":  [999],
    "amount":   [1],
})
table.overwrite(replacement)
```

Now run the consumer again:

```python
try:
    batch_post_overwrite = consumer.run()
except RuntimeError as e:
    print(f"Consumer correctly refused: {e}")
```

The consumer raises because the snapshot's `operation` is `overwrite`, not `append`. **Insight:** any incremental consumer needs an explicit policy for what to do when a non-append commit shows up upstream. Options:

- **Fail loudly** (what we just did) — appropriate for "events should be append-only" contracts
- **Reset and full-rescan** — appropriate when the source table is allowed to be rewritten (e.g., bronze → silver curated tables)
- **Route to a Spark changelog scan** — when you need row-level deletes/updates

### Step 7 — Using PyIceberg's built-in incremental APIs (when available)

Newer PyIceberg versions expose this directly. Probe what your version supports:

```python
table = catalog.load_table("lab.events_stream")
# Probe attributes — names vary by version
for attr in ["incremental_append_scan", "append_incremental", "scan", "inspect"]:
    print(attr, "->", hasattr(table, attr))

# If `incremental_append_scan` exists:
# scan = table.incremental_append_scan(from_snapshot_id=<bookmark>, to_snapshot_id=<latest>)
# df = scan.to_arrow()
```

The hand-rolled consumer above gives you the mechanics without depending on a specific PyIceberg version, which is exactly what an interviewer wants to hear you describe.

### Step 8 — Snapshot expiration: the silent killer

What happens if `expire_snapshots` runs upstream and removes your bookmark snapshot?

```python
table = catalog.load_table("lab.events_stream")

# (Simulate. In real life this would happen via Spark CALL or scheduled job.)
# For the demo: take a snapshot id that we know is not the current one and assume it's been expired.
bookmark = list(table.snapshots())[0].snapshot_id   # an old one

# Try to look it up:
snap = table.snapshot_by_id(bookmark)
print("Snapshot still resolvable:", snap is not None)
```

If you call `expire_snapshots` aggressively and your consumer has been down for longer than the retention window, the bookmark points at a snapshot that **no longer exists**. The consumer can't walk the parent chain. **Production rule:** snapshot retention ≥ max-consumer-downtime × safety factor. See experiment 10 for the policy framework.

---

## 💥 Break it

### Break 1: consume a table that someone compacted

```python
# Have someone (or your Spark profile) run rewrite_data_files on lab.events_stream
# Then run the consumer:
try:
    consumer.run()
except RuntimeError as e:
    print(e)
```

Compaction creates a `replace` snapshot. The naive consumer fails. **Why this matters:** compaction is a *common* upstream operation; you can't ignore it. Production consumers usually filter snapshots by operation type:

```python
ALLOWED = {"append"}
SKIP    = {"replace"}  # data didn't change, just reorganized — skip safely
FAIL    = {"overwrite", "delete"}  # data changed
```

The consumer above blindly fails on everything that isn't append. Make this configurable.

### Break 2: out-of-order processing

Open two notebook kernels. In each, run the consumer concurrently against the same table with the same bookmark store. They'll both fetch the same snapshots, both update the bookmark, both write to the downstream sink. **Duplicates.**

In production, run a single consumer per bookmark, or use a distributed lock (the bookmark store itself can serve — write the bookmark with a conditional update on its current value).

### Break 3: clock skew vs snapshot order

Snapshots have `committed_at` timestamps. Don't use them for ordering. Snapshot IDs are not monotonic by clock — they're monotonic by parent chain. Always order by walking the chain.

```python
# WRONG: relying on committed_at
snaps_wrong = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)

# RIGHT: walk the parent chain
def chain_from_root(table):
    head = table.current_snapshot()
    chain = []
    while head is not None:
        chain.append(head)
        if head.parent_snapshot_id is None:
            break
        head = table.snapshot_by_id(head.parent_snapshot_id)
    return list(reversed(chain))
```

### Break 4: changelog-flavored consumer (Spark-only, optional)

If you have the Spark profile up, see what a full changelog scan looks like:

```bash
cd lab/spark-profile
docker compose exec spark-iceberg spark-sql
```

```sql
-- Generate some history first (run earlier MERGE-style operations against demo.orders_mor from experiment 14)
-- Then:
SELECT * FROM rest_lab.demo.orders_mor.changes
WHERE snapshot_id > <bookmark>;
```

The `.changes` metadata table emits `_change_type` columns (`INSERT`, `DELETE`, `UPDATE_PREIMAGE`, `UPDATE_POSTIMAGE`). This is the proper CDC feed when row-level deletes matter. **The headline:** PyIceberg doesn't expose this yet (as of mid-2026); Spark does. If your consumer is Python-only, your source needs to be append-only or you bolt on a Spark job to materialize the changelog into a downstream Iceberg table that *is* append-only.

---

## 📚 Theory deep-dive

### The snapshot tree

Every commit produces:

```
snapshot:
  snapshot_id:        an opaque long, not time-monotonic
  parent_snapshot_id: pointer to previous snapshot (forms a tree)
  timestamp_ms:       wall-clock at commit (not reliable for ordering)
  summary:
    operation:        append | overwrite | delete | replace
    added-data-files: integer
    added-records:    integer
    ...
  manifest_list:      pointer to the snapshot's manifest list
```

Most tables form a **linear chain** under one branch (default: `main`). Branches and tags (experiment 17) turn this into a tree.

### Three families of incremental read

| Family | What you get | PyIceberg support | When to use |
|---|---|---|---|
| **Append incremental scan** | data files added between `from_snap` and `to_snap` | yes (`incremental_append_scan` or hand-rolled walk) | append-only sources — events, logs, bronze ingestion |
| **Changelog scan** | row-level events with `_change_type` | partial (Spark via `.changes` metadata table; PyIceberg WIP) | CDC sinks where downstream needs to apply UPDATEs and DELETEs |
| **Snapshot-pinned full scan** | the whole table as of a given snapshot | yes (`scan(snapshot_id=…)`) | nightly batch, "as of X" reports, audit reproducibility |

### Bookmark store design

- The bookmark store **must** be transactional with the downstream side-effect when you need exactly-once. Common shapes:
  - Postgres row: `(consumer_id, source_table, last_snapshot_id, updated_at)` updated in the same transaction as the side-effect.
  - Iceberg control table: `lab.consumer_bookmarks` with one row per `(consumer_id, source_table)`. MERGE INTO it. Iceberg ACID gives you durability; the downstream side-effect needs its own idempotency.
  - Kafka offset committed alongside the downstream produce.

- **Bookmarks survive consumer restarts but not snapshot expiration.** Pair this with a snapshot retention policy ≥ max consumer downtime × safety factor (3x is a sensible default).

### Failure modes catalog

| Failure | Symptom | Fix |
|---|---|---|
| Consumer crashes after side-effect, before bookmark commit | duplicates downstream | downstream MERGE/UPSERT by business key; or transactional bookmark |
| Consumer crashes before side-effect, after bookmark commit | missing rows downstream | always commit bookmark **after** side-effect |
| Upstream `expire_snapshots` removed the bookmark | consumer fails to look up bookmark snapshot | extend retention, or detect-and-rescan from earliest available snapshot |
| Upstream compaction (`replace` snapshot) | consumer fails to interpret as append | filter `replace` snapshots as "no new data" |
| Upstream `overwrite` or `delete` | consumer ignores deletes, downstream goes stale | route to changelog scan, or full re-scan |
| Two consumers, same bookmark | duplicates, races | single-consumer constraint, or lock on the bookmark row |

### Streaming vs batch consumers

- **Batch incremental consumer** (the pattern in this experiment): scheduled every N minutes, reads files added since last bookmark, dumps to downstream. Simple, scales to TB.
- **Streaming consumer**: long-running, polls for new snapshots, emits per-batch. Flink's Iceberg source does this. Latency is bounded by upstream commit frequency, not by your poll loop.

In both cases the snapshot chain + bookmark is the underlying mechanism. The streaming consumer just polls more often.

### "Why is this better than reading Kafka directly?"

Common interview follow-up. The honest answer:

- **Replay**: Iceberg keeps the data as long as snapshots aren't expired. Kafka keeps it as long as retention allows (usually shorter).
- **Schema evolution**: Iceberg gives you stable field-ids across upstream schema changes. Kafka topics with Avro/Protobuf give you schema registry — separate system, separate failure mode.
- **Multi-consumer scalability**: every consumer reads Iceberg snapshots independently with no coordination cost. Multiple Kafka consumers in the same group split partitions.
- **Joinability**: Iceberg-as-source means your consumer can join the change feed against other lake tables in the same scan.

The downside: Iceberg's "freshness" is commit-frequency-bound. Upstream commits every 60s → consumer sees data 60s late. Kafka end-to-end can be sub-second.

---

## 🇪🇺 Regulatory angle

**GDPR Art. 17 (right to erasure) + downstream propagation.** When a user submits an erasure request, you delete from the source Iceberg table — but every downstream system that received that row's data via CDC must also drop it. This is where append-only-with-bookmarks falls apart: appending an "erasure event" is not the same as a real CDC delete. Two correct patterns:

1. **Tombstone events**: source emits an explicit `_erasure_request_for(event_id=…)` row in the same table. Downstream consumers interpret it as "delete this row from your sink." This requires a contract — the consumer has to know how to handle the tombstone.
2. **Changelog scan + downstream MERGE**: consumer reads the changelog metadata table (Spark), gets a real DELETE row, MERGEs into its sink with the delete branch. This is the "proper" pattern but requires Spark or equivalent in the consumer.

In a DORA Article 28 (data portability) interview: "Our CDC consumers walk the Iceberg snapshot chain and persist a bookmark transactionally with the downstream write. Snapshot retention is sized to exceed our maximum tolerated consumer downtime by 3x. Erasure events are propagated via the Iceberg changelog metadata table consumed by a Spark job that MERGEs downstream sinks — we do not rely on append-only semantics for erasure."

---

## ✍️ Re-answer the interview question

> "How do you build a CDC consumer on top of an Iceberg table?"

Cover:

1. Snapshot chain: every commit = new snapshot with parent pointer; consumer walks the chain.
2. Bookmark = "last snapshot processed," persisted alongside downstream side-effect for at-least-once.
3. Three incremental flavors: append-incremental (PyIceberg), changelog (Spark only, today), full-pinned (both).
4. The four classes of failure: snapshot expired, compaction, overwrite, concurrent consumers.
5. Tradeoff vs Kafka: better replay & schema evolution; worse freshness.
6. GDPR/erasure handling needs explicit design — append-only does not propagate deletes.

---

## 🎁 LinkedIn post draft

> **"Iceberg makes a brilliant CDC source — but only if you understand snapshot bookmarks. I built a consumer that survived expired snapshots, compaction, and a botched overwrite. Here are the four invariants I'd defend in any architecture review."**
>
> Walk the failure modes table. End with the GDPR/changelog caveat.

---

## Session wrap-up

1. Confirm hands-on: `lab.events_stream` exists, you have a working consumer with a bookmark, you've seen it correctly refuse a non-append snapshot.
2. Update `interview-faq.md`.
3. Reset if needed: `cd lab && ./reset.sh --confirm`.
4. End of day: `docker compose stop`.

---

## Next up

→ [Experiment 16: Production catalog (REST / Lakekeeper)](16-production-catalog-rest.md) — your SQLite catalog has carried you from experiment 1 through 15. Now switch to the catalog you'd actually run in production, and learn why every serious Iceberg deployment in 2026 uses REST.
