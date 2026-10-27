# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect a freshly written shredded VARIANT file on serverless
# MAGIC
# MAGIC This writes a separate inspection dataset from `workspace.default.variant_inspect_shredded` into a Unity Catalog volume. It inspects the **new files**, not the original managed-table files. Databricks may choose a different physical layout on the new write.
# MAGIC
# MAGIC Run with serverless notebook compute, or compatible classic compute. You need permission to read the source and read/write the volume. The first SQL cell creates a volume if absent; use an existing permitted volume instead if you cannot create one. No existing source data is modified, and no cleanup runs automatically.
# MAGIC
# MAGIC Databricks permits path-based Delta data in a volume, but these files must not be registered as a Unity Catalog table. See [volume path rules](https://docs.databricks.com/aws/en/volumes/paths).
# MAGIC
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.variant_inspection;

# COMMAND ----------

from uuid import uuid4

volume_root = "/Volumes/workspace/default/variant_inspection"
inspection_path = f"{volume_root}/shredded_{uuid4().hex}"
source = spark.table("workspace.default.variant_inspect_shredded").select(
    "sample_id", "payload"
).limit(10000)

# Create an empty, unregistered Delta dataset. This gives ALTER a target
# before any sample records are written.
source.limit(0).write.format("delta").mode("errorifexists").save(inspection_path)

spark.sql(f"""
    ALTER TABLE delta.`{inspection_path}`
    SET TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
""")

details = spark.sql(f"DESCRIBE DETAIL delta.`{inspection_path}`").first()
assert details["properties"].get("delta.enableVariantShredding", "").lower() == "true"

# Coalescing is only to keep this small inspection example manageable.
source.coalesce(1).write.format("delta").mode("append").save(inspection_path)
print("Inspection dataset:", inspection_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print the actual Parquet schema
# MAGIC
# MAGIC The dataset is new and unpartitioned. We inspect its top-level Parquet files and ignore any empty file produced while initializing it. This simple listing is suitable for this fresh dataset, not a general way to find active files in a changing Delta table.
# MAGIC
# MAGIC If PyArrow cannot interpret the file's annotations, use a compatible PyArrow dependency in the notebook environment. A reader error is not evidence that shredding is absent.

# COMMAND ----------

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

print("PyArrow version:", pa.__version__)
if int(pa.__version__.split(".")[0]) < 22:
    raise RuntimeError(
        "This inspection example uses PyArrow 22 or newer. "
        "On serverless, add pyarrow==22.0.0 in Environment > Dependencies, "
        "then Apply. Save your existing file path before the Python restart; "
        "rerun only the inspection cells, not the data creation cells."
    )

parquet_files = sorted(Path(inspection_path).glob("*.parquet"))
nonempty_files = []
column_rows = []

for file in parquet_files:
    pf = pq.ParquetFile(str(file), arrow_extensions_enabled=False)
    if pf.metadata.num_rows == 0:
        continue
    nonempty_files.append(file)
    print(f"\nFILE: {file.name}\n{pf.schema}")
    for i in range(len(pf.schema)):
        leaf = pf.schema.column(i)
        column_rows.append((
            file.name,
            leaf.path,
            leaf.physical_type,
            str(leaf.logical_type),
        ))

if not nonempty_files:
    raise RuntimeError("No nonempty Parquet files found in this inspection dataset.")

display(spark.createDataFrame(
    column_rows,
    "file STRING, physical_path STRING, physical_type STRING, logical_type STRING",
).orderBy("file", "physical_path"))

if not any("typed_value" in row[1].split(".") for row in column_rows):
    print("No typed_value leaf was found. The property is enabled, but these files do not show typed shredding. Inspect the stored schema before choosing a physical field path.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect one stored row
# MAGIC
# MAGIC Look for `metadata` (binary dictionary), `value` (retained encoded content), and nested `typed_value` groups. Missing binary content can appear as `None` when that data is represented by typed fields. Group layout varies; this notebook does not assume a fixed number or list of shredded paths.
# MAGIC
# MAGIC The footer above is the authoritative schema evidence. If your Arrow version automatically converts VARIANT into a logical extension object, the row below might not expose its raw groups; that is a reader behavior, not a change in the stored file.

# COMMAND ----------

from pprint import pprint

pf = pq.ParquetFile(str(nonempty_files[0]), arrow_extensions_enabled=False)
batch = next(pf.iter_batches(batch_size=1))
stored_row = batch.to_pylist()[0]
pprint(stored_row, width=110, sort_dicts=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read one physical leaf from the observed schema
# MAGIC
# MAGIC Set `physical_leaf` using an exact path from the displayed inventory, then run this cell. This can show a typed amount or binary fallback directly. Use the file’s schema inventory to select the path.

# COMMAND ----------

physical_leaf = ""
if physical_leaf:
    known_paths = {pf.schema.column(i).path for i in range(len(pf.schema))}
    if physical_leaf not in known_paths:
        raise ValueError("Choose a leaf from the schema of the first inspected file.")
    batch = next(pf.iter_batches(batch_size=1, columns=[physical_leaf]))
    pprint(batch.to_pylist(), width=110, sort_dicts=False)
else:
    print("Set physical_leaf to an observed metadata, value, or typed_value leaf path.")

# COMMAND ----------

# MAGIC %md
# MAGIC Sources: [Databricks shredding](https://docs.databricks.com/aws/en/tables/features/variant-shredding), [Parquet shredding format](https://parquet.apache.org/docs/file-format/types/variantshredding/), [PyArrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html).
