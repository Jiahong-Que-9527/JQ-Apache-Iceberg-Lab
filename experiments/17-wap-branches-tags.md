# Experiment 17 — Write-Audit-Publish with Branches & Tags

> **Time:** 120 min · **Tier:** Production · **Prerequisites:** Experiments 01–10, 14 strongly recommended · **Spark profile required**

---

## 🎯 The interview question this answers

> **"Walk me through Write-Audit-Publish in Iceberg with branches and tags. How do you fast-forward main? How do you roll back a bad publish? How do tags differ from branches?"**

Pause. Open `interview-faq.md`. Write your current answer.

**Why this matters:** WAP is the single most powerful data-quality pattern in modern lakehouses. It's also where most teams' first answer is wrong ("we write to a staging table and copy if it's good") because they haven't seen Iceberg's native primitives. Knowing the branch-based version cleanly signals "I've actually built this."

---

## TL;DR

Iceberg V2 supports **refs** — named pointers to snapshots. Two flavors:

- **Branches** — mutable, advance forward as you write to them. `main` is just a branch.
- **Tags** — immutable, pinned to a single snapshot. `eod_2026_05_18` is a tag.

WAP is: **Write** to a branch, **Audit** (run quality checks reading that branch), **Publish** by fast-forwarding `main` to the branch tip. If audit fails, abandon the branch — no rollback needed because `main` was never touched. Tags are the audit trail: an immutable marker for every published state, kept long after the branch is gone.

---

## 🛠️ Hands-on: build a WAP pipeline

### Step 0 — Spark profile up

```bash
cd lab/spark-profile && ./up.sh
docker compose exec spark-iceberg spark-sql
```

### Step 1 — Create a production-shaped table

```sql
CREATE NAMESPACE IF NOT EXISTS wap;
DROP TABLE IF EXISTS wap.payments;

CREATE TABLE wap.payments (
    payment_id   BIGINT,
    user_id      BIGINT,
    amount       DECIMAL(10,2),
    currency     STRING,
    paid_at      TIMESTAMP,
    status       STRING
)
USING iceberg
PARTITIONED BY (days(paid_at))
TBLPROPERTIES ('format-version' = '2');

-- Seed with "yesterday's" data — already published, already trusted
INSERT INTO wap.payments VALUES
    (1, 100, 49.99, 'EUR', TIMESTAMP '2026-05-17 09:00:00', 'completed'),
    (2, 101, 19.99, 'EUR', TIMESTAMP '2026-05-17 10:30:00', 'completed'),
    (3, 102, 99.50, 'EUR', TIMESTAMP '2026-05-17 14:00:00', 'completed');

SELECT * FROM wap.payments ORDER BY payment_id;
```

Tag this baseline so we can always come back:

```sql
ALTER TABLE wap.payments CREATE TAG baseline_2026_05_17;

SELECT name, snapshot_id, type FROM wap.payments.refs;
```

### Step 2 — Create an audit branch and write to it

```sql
-- Create a branch from the current snapshot
ALTER TABLE wap.payments CREATE BRANCH audit_2026_05_18;

SELECT name, snapshot_id, type FROM wap.payments.refs;
```

You now have two refs pointing at the same snapshot: `main` and `audit_2026_05_18`. Set the *session* default branch to the audit branch so subsequent writes go there:

```sql
SET spark.wap.branch = audit_2026_05_18;
-- (Alternative explicit form: WRITE TO BRANCH audit_2026_05_18 ... — but Spark Iceberg supports both)

INSERT INTO wap.payments VALUES
    (4, 100, 14.99, 'EUR', TIMESTAMP '2026-05-18 09:00:00', 'completed'),
    (5, 103,  4.99, 'EUR', TIMESTAMP '2026-05-18 09:15:00', 'completed'),
    (6, 104, 29.00, 'USD', TIMESTAMP '2026-05-18 09:20:00', 'completed'),
    -- One row we want our audit to catch
    (7, 105, -1.00, 'EUR', TIMESTAMP '2026-05-18 09:30:00', 'completed');
```

### Step 3 — Audit by reading the branch

```sql
-- main is unchanged
SELECT COUNT(*) AS main_rows FROM wap.payments;
-- expect 3 (only yesterday's data)

-- audit branch has the new data
SELECT COUNT(*) AS branch_rows FROM wap.payments VERSION AS OF 'audit_2026_05_18';
-- expect 7
```

**Insight:** readers querying `wap.payments` see only the published state. The new data is invisible to the production read path until we publish.

Now run the audit. Define your data-quality assertions and check them against the branch:

```sql
WITH branch AS (
  SELECT * FROM wap.payments VERSION AS OF 'audit_2026_05_18'
)
SELECT
  COUNT(*) FILTER (WHERE amount <= 0)              AS negative_amounts,
  COUNT(*) FILTER (WHERE currency NOT IN ('EUR','USD','GBP')) AS unknown_currencies,
  COUNT(*) FILTER (WHERE paid_at < DATE '2020-01-01')         AS suspicious_dates,
  COUNT(*) FILTER (WHERE user_id IS NULL OR payment_id IS NULL) AS missing_keys
FROM branch;
```

You should see `negative_amounts = 1`. **Audit failed.** In a real pipeline this is where you'd:

- Page an on-call, OR
- Quarantine the bad row + retry, OR
- Abandon the branch and rerun upstream.

Choose abandon for this demo.

### Step 4 — Abandon the failed branch

```sql
ALTER TABLE wap.payments DROP BRANCH audit_2026_05_18;

SELECT name, snapshot_id, type FROM wap.payments.refs;
-- audit_2026_05_18 should be gone
SELECT COUNT(*) FROM wap.payments;
-- still 3 — main was never touched
```

**Insight:** dropping the branch removes only the ref. The snapshot it pointed at, and its data files, still exist (garbage-collectable later by `expire_snapshots`). No partial state ever leaked to readers.

### Step 5 — Retry with corrected data

```sql
ALTER TABLE wap.payments CREATE BRANCH audit_2026_05_18;
SET spark.wap.branch = audit_2026_05_18;

INSERT INTO wap.payments VALUES
    (4, 100, 14.99, 'EUR', TIMESTAMP '2026-05-18 09:00:00', 'completed'),
    (5, 103,  4.99, 'EUR', TIMESTAMP '2026-05-18 09:15:00', 'completed'),
    (6, 104, 29.00, 'USD', TIMESTAMP '2026-05-18 09:20:00', 'completed'),
    (7, 105,  1.00, 'EUR', TIMESTAMP '2026-05-18 09:30:00', 'completed');  -- fixed

WITH branch AS (SELECT * FROM wap.payments VERSION AS OF 'audit_2026_05_18')
SELECT COUNT(*) FILTER (WHERE amount <= 0) AS negative_amounts FROM branch;
-- 0 — audit passes
```

### Step 6 — Publish by fast-forwarding `main`

```sql
CALL rest_lab.system.fast_forward('wap.payments', 'main', 'audit_2026_05_18');

SET spark.wap.branch = main;  -- back to normal

SELECT COUNT(*) FROM wap.payments;
-- 7 — published
```

**Now readers see the new data**, all four rows visible *atomically*, with no in-between state. The transition was: snapshot S0 (3 rows, baseline) → snapshot S1 (7 rows, post-audit). No reader ever saw 4, 5, or 6 rows.

Tag this published state:

```sql
ALTER TABLE wap.payments CREATE TAG eod_2026_05_18;

SELECT name, snapshot_id, type, max_snapshot_age_ms, min_snapshots_to_keep
FROM wap.payments.refs
ORDER BY type, name;
```

### Step 7 — Drop the audit branch, keep the tag

```sql
ALTER TABLE wap.payments DROP BRANCH audit_2026_05_18;
```

The published state is now reachable two ways: through `main` (mutable, will move forward tomorrow) and through `eod_2026_05_18` (immutable, will point at this exact snapshot forever — until you explicitly drop the tag).

### Step 8 — Audit branch retention policies

Branches and tags have retention metadata. Audit branches should expire fast; EOD tags should live for the regulatory retention period.

```sql
ALTER TABLE wap.payments CREATE BRANCH audit_2026_05_19
RETAIN 3 DAYS                  -- the branch ref itself is dropped after 3 days
WITH SNAPSHOT RETENTION 1 SNAPSHOTS;  -- and only the latest snapshot on the branch is kept

ALTER TABLE wap.payments CREATE TAG eod_2026_05_19
RETAIN 2555 DAYS;              -- 7 years (MiFID II / DORA territory)
```

`expire_snapshots` respects these — a snapshot is only eligible for deletion if no live ref keeps it alive. **The ref retention policy IS your time-travel guarantee.**

### Step 9 — Rolling back a bad publish

You published, then realized the data was wrong. Two options.

**Option A: rollback `main` to a previous snapshot** (works if no one wrote after you):

```sql
-- See the history
SELECT made_current_at, snapshot_id, parent_id, operation FROM wap.payments.history;

-- Roll main back to the baseline
CALL rest_lab.system.rollback_to_snapshot(
    table       => 'wap.payments',
    snapshot_id => (SELECT snapshot_id FROM wap.payments.refs WHERE name = 'baseline_2026_05_17')
);

SELECT COUNT(*) FROM wap.payments;
-- 3 — back to baseline
```

**Option B: cherry-pick away the bad commit** (when later writes are good and you want to keep them, just remove the bad one). Iceberg supports `cherrypick_snapshot` to apply a snapshot from one branch to another:

```sql
-- Create a clean branch from baseline, cherry-pick only the good commits, then fast-forward main onto it
ALTER TABLE wap.payments CREATE BRANCH recovery FROM TAG baseline_2026_05_17;
-- (cherry-pick logic here; in practice this is rare and tool-assisted)
```

Roll back to the published state for the rest of the experiment:

```sql
CALL rest_lab.system.fast_forward('wap.payments', 'main', (SELECT snapshot_id FROM wap.payments.refs WHERE name='eod_2026_05_18'));
```

Wait — `fast_forward` requires the target to be an ancestor. Use the lower-level set-current-snapshot if needed:

```sql
CALL rest_lab.system.set_current_snapshot(
    table => 'wap.payments',
    snapshot_id => (SELECT snapshot_id FROM wap.payments.refs WHERE name='eod_2026_05_18')
);
```

### Step 10 — A second engine should not see the audit branch by accident

Switch to Trino:

```bash
docker compose exec trino trino
```

```sql
SELECT count(*) FROM iceberg.wap.payments;          -- 7, the published state
-- Branch-aware reads (Trino: VERSION AS OF '<branch>')
SELECT count(*) FROM iceberg.wap.payments FOR VERSION AS OF 'eod_2026_05_18';
```

`main` is what every engine reads by default. The branch was strictly an internal write-side concept. **No reader ever needs to know audit branches exist** — that's the whole UX win of WAP.

---

## 💥 Break it

### Break 1: forget to fast-forward; readers are stuck on yesterday

Skip the `fast_forward` step after a successful audit. Days later, queries still show old data. The audit branch has all the new commits, but `main` hasn't moved. **Insight:** the publish step is load-bearing and *separate from* the audit. A WAP pipeline needs an explicit "publish" stage that is monitored — orphan audit branches mean stale data downstream.

### Break 2: concurrent writers, one on each branch

In two spark-sql sessions:

```sql
-- session A
SET spark.wap.branch = audit_v1;
INSERT INTO wap.payments VALUES ...;
```

```sql
-- session B
SET spark.wap.branch = audit_v2;
INSERT INTO wap.payments VALUES ...;
```

Two branches diverge cleanly. No conflict at commit time — each branch is independent. **Insight:** this is the *only* place in Iceberg where two writers can safely make progress on the "same table" without OCC retries. Branches give you write-side isolation.

### Break 3: `fast_forward` when branches have diverged

If `main` got new commits during your audit (e.g., a hotfix landed directly on main), the fast-forward will fail because `audit_2026_05_18` is no longer a descendant of `main`'s head.

```sql
CALL rest_lab.system.fast_forward('wap.payments', 'main', 'audit_2026_05_18');
-- ERROR: 'main' is not an ancestor of 'audit_2026_05_18'
```

**Insight:** WAP assumes serial publish. If you want parallel publishes you need to either rebase your branch (cherry-pick) or coordinate at a higher layer.

### Break 4: branch on a snapshot that's been expired

Set a too-aggressive `expire_snapshots` policy and watch your audit branch's source snapshot disappear:

```sql
CALL rest_lab.system.expire_snapshots(table => 'wap.payments', older_than => current_timestamp());
```

After this, branches referencing expired snapshots are in an inconsistent state. Most Iceberg versions protect refs (a snapshot held by a ref is not eligible for expiration), but **only if the ref existed at expire time**. **Production rule:** branches and tags must exist *before* you run expiration, or expiration must read the refs table and exclude them.

### Break 5: a tag is not a backup

Tags pin to a snapshot. The data files behind that snapshot live on as long as the tag does. If you drop the tag, the data files become eligible for cleanup on the next `expire_snapshots`. **A tag is a *ref-counted reference*, not a copy.** Tags do not protect against bucket-level deletes, regional storage failures, or S3 lifecycle policies aimed at the prefix.

---

## 📚 Theory deep-dive

### The refs model

Every Iceberg table maintains a `refs` table (visible via `<table>.refs`):

| Column | Meaning |
|---|---|
| `name` | the ref name (`main`, `audit_…`, `eod_…`) |
| `type` | `BRANCH` or `TAG` |
| `snapshot_id` | the snapshot it points at |
| `min_snapshots_to_keep` | branch only: how many snapshots on this branch to keep |
| `max_snapshot_age_ms` | branch only: how old snapshots on this branch can get |
| `max_ref_age_ms` | for both: when the ref itself becomes droppable |

`main` is a branch with no retention bounds by default. Every commit to "the table" is implicitly a commit to `main`.

### WAP without branches (the old way)

Before refs landed in V2, teams did WAP via separate "staging" tables:

```
write       → wap_payments_staging
audit       → SELECT … FROM wap_payments_staging
publish     → INSERT INTO wap_payments SELECT * FROM wap_payments_staging
```

Problems:
- **Cost**: 2× storage and 2× write IO during the publish step.
- **Atomicity**: the publish itself isn't atomic. Readers can hit the table mid-INSERT.
- **Schema drift**: staging and prod schemas can diverge.

Branches solve all three: zero copy, atomic ref swap, single schema.

### Fast-forward semantics

Fast-forward only succeeds if the target branch's head is an ancestor of the source branch's head. The catalog atomically updates the target ref to point at the source's head snapshot. No data moves. The reader sees the entire delta atomically.

If branches have diverged, you need a merge — Iceberg doesn't do automatic merges. Either rebase the audit branch (cherry-pick), or coordinate at the orchestrator level.

### Tags as audit trail

The standard production pattern:

| Phase | Refs in play |
|---|---|
| Each ETL run | create `audit_YYYY_MM_DD` branch from `main` |
| Audit passes | fast-forward `main` to branch head, create `eod_YYYY_MM_DD` tag at that snapshot, drop the audit branch |
| Audit fails | drop the audit branch, no tag created |
| Daily | a clean `main` + a chain of `eod_*` tags |
| Regulatory window expires | drop oldest tags; `expire_snapshots` reclaims storage |

This gives auditors a clean "show me the state on 2026-05-18 at end of day" query: `SELECT * FROM wap.payments VERSION AS OF 'eod_2026_05_18'`.

### Branch retention vs snapshot retention

Two different things, often confused:

- **Snapshot retention** (`history.expire_snapshots`) — when does a snapshot's data become eligible for deletion?
- **Branch/tag retention** (`max_ref_age_ms`, `min_snapshots_to_keep`) — when does the *ref itself* get dropped, and how many snapshots reachable from the ref are protected?

If a snapshot is reachable from any live branch or tag, it's protected from expiration. So branch/tag retention is the *upstream* knob; snapshot retention is the *downstream* knob.

### What branches don't give you

- **Cross-table atomicity**: you can fast-forward five branches in sequence but each is its own atomic op. If the third one fails, the first two already published. For real cross-table transactions you need Nessie.
- **Mergeable changes**: branches are independent timelines. No three-way merge. If you've diverged, you must rebase.
- **Implicit garbage collection**: dropping a branch doesn't immediately delete data. `expire_snapshots` is still the cleanup pass.

---

## 🇪🇺 Regulatory angle

**DORA Art. 6 (ICT risk management) + Art. 9 (integrity)**: WAP gives you a defensible technical control. Every publish is a deliberate, audited, reversible operation. Quality gates are embedded in the data plane, not just the orchestrator. Failed audits leave a trail (the abandoned branch's snapshot is still reachable in history until expiration), which an auditor can inspect to verify "yes, the firm detected this issue and refused to publish."

**MiFID II RTS 25 (reproducibility)**: tags at end-of-day are the literal regulatory artifact. `SELECT * FROM trades VERSION AS OF 'eod_2026_05_18'` is the answer to "show me the state of the trade ledger at close of business on 18 May 2026" — verbatim, reproducible, immutable.

**BaFin BAIT (access controls)**: combined with REST catalog vended creds (experiment 16), branches let you grant "write to audit_* branches, read main, no fast_forward without manager approval" — segregation of duties enforced at the catalog level.

In an interview: "WAP on Iceberg gives us a four-eyes control on data publishing without any custom orchestration: the writer creates the audit branch, the auditor (human or automated) checks it, the publisher does the fast-forward. The tag created at publish is the BaFin-defensible artifact of what was approved and by whom — recorded in snapshot summaries plus our orchestrator's audit log."

---

## ✍️ Re-answer the interview question

> "Walk me through WAP. How do you fast-forward? How do you roll back? How are tags different?"

Cover:

1. **Refs model**: branches (mutable) and tags (immutable), all snapshots
2. **WAP loop**: create branch → write to branch → audit on branch → fast-forward main → tag → drop branch
3. **Atomicity**: readers see the entire delta atomically when main moves; never partial state
4. **Failure handling**: drop the branch, main is untouched
5. **Rollback**: `rollback_to_snapshot` or fast-forward main to an earlier tag
6. **Tag retention** = your regulatory artifact lifetime; **branch retention** = your audit-window lifetime
7. **Limits**: no cross-table atomicity, no three-way merges

---

## 🎁 LinkedIn post draft

> **"Most teams build Write-Audit-Publish with two tables and a copy step. Iceberg branches make it zero-copy, atomic, and free. Here's the pattern I'd defend in any platform review."**
>
> Walk the loop. End with the regulatory tag angle — "the tag is the artifact your auditor asks for."

---

## Session wrap-up

1. Confirm hands-on: `wap.payments` exists with a baseline tag + an eod tag, you've run a failed audit and abandoned the branch, you've run a passing audit and published via fast-forward, you've inspected `.refs`.
2. Update `interview-faq.md`.
3. Spark profile down: `cd lab/spark-profile && ./down.sh`. Main lab can stay running.

---

## Next up

→ [Experiment 18: Maintenance & observability](18-maintenance-observability.md) — now that you can write, mutate, version, and publish, you need to see what's happening inside the tables. Iceberg ships an entire SQL surface for table introspection that most teams underuse.
