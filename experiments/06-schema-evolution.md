# Experiment 06 — Schema Evolution

> **Time:** 75 min · **Tier:** Mechanics · **Prerequisites:** Experiments 01–05

---

## 🎯 The interview question this answers

> **"Why is Iceberg schema evolution safe when raw Parquet's isn't? What's a field-id and why does it matter?"**

Write your current answer in `interview-faq.md`.

**Why this matters:** This is the question that proves you understand *why* Iceberg exists at all. Parquet has had schema evolution for years — but in dangerous ways. Iceberg's field-id system is one of the most thoughtful designs in the entire spec.

---

## TL;DR

Parquet matches columns by **name**. Rename a column = lose your data. Iceberg matches columns by **field-id** (a stable integer assigned at create time). Rename, reorder, add, drop — old data files still readable, no rewrite needed. **This single design decision is why Iceberg is safer than raw Parquet for evolving schemas.**

---

## 🛠️ Hands-on: evolve a schema four ways

### Step 1 — Create a table and write data

```bash
./reset.sh
```

```python
from src.catalog_helper import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, DoubleType
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa

catalog = get_catalog()
catalog.create_namespace_if_not_exists("lab")

schema = Schema(
    NestedField(1, "user_id", LongType(), required=True),
    NestedField(2, "name", StringType()),
    NestedField(3, "balance", DoubleType()),
)
table = catalog.create_table("lab.accounts", schema=schema)

table.append(pa.table({
    "user_id": [1, 2, 3],
    "name":    ["Alice", "Bob", "Carol"],
    "balance": [100.0, 200.0, 300.0],
}))

print("Initial schema:")
print(table.schema())
```

Note the field-ids. They are assigned at creation: user_id=1, name=2, balance=3.

### Step 2 — Add a column

```python
with table.update_schema() as us:
    us.add_column("country", StringType())

table = catalog.load_table("lab.accounts")
print("After add_column:")
print(table.schema())

# The new column should have field_id=4 (next available)
```

Now read existing data:

```python
print(table.scan().to_arrow().to_pandas())
```

The old rows have `null` for the new `country` column. **Important: no data was rewritten.** Look at MinIO — the original Parquet file is still there. Iceberg just told readers "if you don't find field-id 4 in this file, return null."

Write some new data with the new column:

```python
table.append(pa.table({
    "user_id": [4, 5],
    "name":    ["Dave", "Eve"],
    "balance": [400.0, 500.0],
    "country": ["DE", "NL"],
}))

table = catalog.load_table("lab.accounts")
print(table.scan().to_arrow().to_pandas())
```

You'll see old rows (1, 2, 3) with `country=None` and new rows (4, 5) with the country populated. **One table, two physical schemas, transparent to the reader.**

### Step 3 — Rename a column (the killer feature)

```python
with table.update_schema() as us:
    us.rename_column("name", "full_name")

table = catalog.load_table("lab.accounts")
print("After rename:")
print(table.schema())
print()
print(table.scan().to_arrow().to_pandas())
```

The column is now `full_name` everywhere — including for the old rows written before the rename. **The data files were not rewritten.** They still contain a column named `name` internally — but the metadata says "field-id 2 is now called `full_name`," so readers see the new name.

This is the magic. Let's prove it. Download the Parquet file directly and inspect its schema:

```python
import boto3, os
from io import BytesIO
import pyarrow.parquet as pq

s3 = boto3.client("s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

# List data files
data_files = [f for f in table.scan().plan_files()]
first_file_path = data_files[0].file.file_path
print(f"Inspecting: {first_file_path}")

bucket = "warehouse"
key = first_file_path.replace(f"s3://{bucket}/", "")
obj = s3.get_object(Bucket=bucket, Key=key)
parquet_file = pq.ParquetFile(BytesIO(obj["Body"].read()))

print("\nActual Parquet schema (note the field metadata!):")
print(parquet_file.schema)
```

Look at the schema. You should see fields named like `name` (the old name!) **with metadata containing the field-id**. The Parquet file still has the old column name. Iceberg's metadata is what makes the rename appear consistent to readers.

### Step 4 — Drop a column

```python
with table.update_schema() as us:
    us.delete_column("balance")

table = catalog.load_table("lab.accounts")
print("After drop:")
print(table.schema())
print(table.scan().to_arrow().to_pandas())
```

Column gone from the schema. But again — **the data files still contain the column**. Iceberg just doesn't read it. If you re-add a column called `balance` later, it gets a new field-id (5, not 3), and old data shows null — because Iceberg matches by ID, not name.

### Step 5 — Promote a type

Some type changes are safe; others aren't.

Safe (widening): `int -> bigint`, `float -> double`, `decimal(P,S) -> decimal(P',S)` where P' >= P.
Unsafe: anything that could lose information.

```python
# Add an int column, then widen it
with table.update_schema() as us:
    us.add_column("login_count", LongType())

table = catalog.load_table("lab.accounts")
table.append(pa.table({
    "user_id": [6], "full_name": ["Frank"], "country": ["FR"], "login_count": [42],
}))

# Now try to "narrow" — should fail
try:
    with table.update_schema() as us:
        us.update_column("login_count", IntegerType())  # may not be importable in this version
except Exception as e:
    print(f"Narrowing failed (as expected): {e}")
```

The exact API for type promotion in pyiceberg varies by version — the principle is what matters: **Iceberg enforces safe type changes only**. You cannot accidentally lose precision.

### Step 6 — See all the schemas in the metadata

```python
table = catalog.load_table("lab.accounts")
# All historical schemas live in metadata.json
import json
metadata = json.loads(open_metadata_file(table))  # use the helper from exp02

print("Schemas in this table's history:")
for sch in metadata["schemas"]:
    print(f"  schema-id {sch['schema-id']}: {len(sch['fields'])} fields")
    for f in sch['fields']:
        print(f"    [{f['id']}] {f['name']}: {f['type']}")
```

You should see multiple schemas: the original, then after each evolution. **Iceberg keeps the full schema history.** When reading an old snapshot, it uses the schema valid at that snapshot's commit.

---

## 💥 Break it

### Break 1: try to add a required column with no default

```python
try:
    with table.update_schema() as us:
        us.add_column("email", StringType(), required=True)
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

You can't add a `required` column without a default — because existing rows have no value for it. **Insight:** Iceberg's schema rules prevent you from creating invalid table states. You'd have to add it as nullable, backfill, then optionally promote to required (in a future version).

### Break 2: drop a partition column

If a column is part of the partition spec, dropping it should fail or be restricted. Set up the test:

```python
# Reset and create a partitioned table
# (We'll do partitioning fully in exp07, this is a preview)
```

Or simpler: try the schema evolution operation that violates a constraint and see the error.

### Break 3: write data with mismatched column types

```python
# Try to append a string where a long is expected
try:
    table.append(pa.table({
        "user_id": ["not_a_number"],  # wrong type
        "full_name": ["Test"],
    }))
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
```

Iceberg validates the input schema against the table schema. **Insight:** type safety is enforced at write time, not at read time. Catches errors at the right place.

---

## 📚 Theory deep-dive

### Why field-ids exist

Imagine a Parquet table managed by Hive. You have files written over six months. You rename a column from `customer_name` to `client_name`. What happens?

- Hive Metastore tracks the new name
- Parquet files still have the old name embedded
- Queries match by name → old files return `null` for `client_name`
- You **silently lose the data** until you rewrite every file

This is a real bug. Many teams have hit it. The "fix" is to rewrite the entire table — TB of data, hours of compute, lots of risk.

Iceberg's solution: **assign a stable integer ID to every field at creation**. Parquet files carry the IDs in column metadata. The "name" you see in SQL is just a label in Iceberg's metadata. Renaming = updating the label, not touching files.

### The rules of safe schema evolution

Iceberg's spec defines which operations are safe:

| Operation | Safe? | Why |
|---|---|---|
| Add column (optional) | ✅ | Old rows return null |
| Add column (required) | ❌ | Old rows have no value |
| Rename column | ✅ | Label change, ID stable |
| Reorder columns | ✅ | Order is just metadata |
| Drop column | ✅ | Old files still have the data, just ignored |
| Re-add dropped column with same name | ⚠️ | Gets a NEW field-id — old data NOT recovered |
| Widen int → bigint | ✅ | Safe upcast |
| Widen float → double | ✅ | Safe upcast |
| Narrow bigint → int | ❌ | Could lose data |
| Change int → string | ❌ | Requires explicit cast |
| Promote required → optional | ✅ | Strictly weaker constraint |
| Demote optional → required | ⚠️ | Only if no existing nulls |

Memorize this table. It's the kind of detail that wins respect in interviews.

### Nested types and field-ids

Field-ids extend into structs, lists, and maps:

```python
NestedField(field_id=5, name="address", field_type=StructType(
    NestedField(field_id=6, name="street", field_type=StringType()),
    NestedField(field_id=7, name="city", field_type=StringType()),
))
```

You can evolve nested fields too — rename `street`, drop `city`, add `zip_code`, all without rewriting data. **This is huge for event tables with rich event payloads** that evolve over time.

### How readers handle multiple schemas

When you query an Iceberg table:

1. Iceberg gives you a **read schema** (the current one, or one you specified)
2. For each data file, Iceberg knows which historical schema it was written with
3. For each column in the read schema, Iceberg looks up the field-id and finds the matching field in the file's schema
4. If found: read the column
5. If not found: return null (for optional columns) or apply a default (for new required-with-default columns)

This translation happens transparently. Engines like DuckDB, Spark, Trino all do it the same way because they follow the Iceberg spec.

### The "re-add deleted column" gotcha

Watch out for this in interviews:

```sql
ALTER TABLE t DROP COLUMN email;       -- field-id 4 dropped
-- ... time passes, you forget ...
ALTER TABLE t ADD COLUMN email STRING; -- new column gets field-id 8
```

The new `email` column is a **completely different column** from the old one. Old data files have field-id 4 marked as dropped; the new field-id 8 isn't in them. **Querying email on old rows returns null, not the original email data.**

This is by design (safer than the alternative), but it surprises people. Knowing this trap = senior-level signal.

---

## 🇪🇺 Regulatory angle

Schema evolution is directly relevant to **MiFID II reporting changes** and **DORA Article 28 data portability**.

**MiFID II RTS 22** changes its required fields periodically. Banks must adapt their internal data structures without losing the ability to reproduce historical reports. Iceberg's schema evolution + snapshot retention is *the* technical answer:

- Add new field → old reports unaffected, new reports include it
- Field renamed by regulator → rename in Iceberg, all historical data still queryable under new name
- Field semantics changed → tag the schema-change snapshot, rerun affected reports off the new snapshot, keep old snapshots for compliance audit

**DORA Article 28** (ICT third-party risk) requires that you can move your data to another provider. Iceberg's field-ids and stable schema history mean the data is **semantically portable** — another platform reading the same files gets the same answers. Compare to a system where renamed columns silently break — that's a regulator's nightmare.

If asked about schema management in a regulated context: **"Iceberg's field-id system is the safest schema evolution model in the lakehouse space. Combined with snapshot tagging, it lets us evolve schemas to track regulatory changes without ever losing the ability to reproduce a historical report."**

---

## ✍️ Re-answer the interview question

> "Why is Iceberg schema evolution safe when Parquet's isn't?"

Cover:

1. Parquet matches by column name → renames break old files silently
2. Iceberg assigns stable field-ids at creation; Parquet files carry these in metadata
3. Schema operations are pure metadata changes — no data rewrite
4. The full schema history is retained; readers use the right schema for each snapshot
5. The re-add-deleted-column gotcha (bonus: shows you've used it for real)

---

## 🎁 LinkedIn post draft

> **"I renamed a column on a Parquet-based table. The data disappeared. Here's the Iceberg fix I wish I had then."**
>
> War-story framing always works. Walk through the field-id mechanism. Bonus: include the Parquet file metadata screenshot showing the embedded field-id.

---

## Next up

→ [Experiment 07: Hidden partitioning](07-hidden-partitioning.md) — why Hive partition columns are an antipattern, and how Iceberg fixes it.
