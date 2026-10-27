# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect VARIANT storage in Parquet
# MAGIC 
# MAGIC Run on **Databricks Runtime 17.3+**, preferably **18.1+** for statistics/data-skipping investigation. Use a scratch catalog/schema where you can create tables. The SQL cells use the notebook's current catalog and schema. Do not point the examples at production tables.
# MAGIC 
# MAGIC This notebook creates two new tables; it does not overwrite or drop existing tables. If the names already exist, choose fresh names consistently. The full A-1001 sample is embedded, so no sample upload is required.
# MAGIC 
# MAGIC The sample is repeated 10,000 times with an outer `sample_id`. These are repeated copies of one order, not 10,000 distinct business orders. This helps inspect a consistent shape; it is **not a performance benchmark** or a guarantee of which fields Databricks chooses to shred.
# MAGIC 
# MAGIC SQL reveals properties, logical values and query plans. The optional Python cells inspect an **original Parquet file**, which is the evidence for the physical layout. `DESCRIBE TABLE` and `schema_of_variant` do not list physical shredded fields.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Same data, different storage settings
# MAGIC 
# MAGIC The timestamp-looking fields remain JSON strings. Shredding is not an instruction to convert them into timestamps or to force money into a particular decimal type.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW variant_inspect_seed AS
# MAGIC SELECT id AS sample_id, parse_json('{"event_id":"evt-1001","schema_version":1,"event_type":"order_placed","occurred_at":"2026-10-20T10:00:00Z","order":{"id":"A-1001","currency":"EUR","customer":{"id":"C-42","tier":"gold"},"total":{"base_total":41.5,"tax_pct":20.0,"tax_amount":8.3},"items":[{"line_id":"L-1","product":{"sku":"MOUSE-01","category":"electronics","attributes":{"connectivity":["bluetooth","usb"],"compatibility":{"operating_systems":["Windows","macOS"]}}},"quantity":1,"pricing":{"unit_price":29.0,"breakdowns":[{"type":"tax","amount":5.8,"detailed_breakdowns":[{"type":"VAT","rate_pct":20.0,"amount":5.8}]}]}},{"line_id":"L-2","product":{"sku":"TSHIRT-02","category":"clothing","attributes":{"size":"M","material":{"cotton_pct":100},"care_instructions":["wash_cold","line_dry"]}},"quantity":1,"pricing":{"unit_price":12.5,"breakdowns":[{"type":"tax","amount":2.5,"detailed_breakdowns":[{"type":"VAT","rate_pct":20.0,"amount":2.5}]}]}}],"fulfillment":{"shipments":[{"id":"S-1","item_references":[{"line_id":"L-1","quantity":1}],"tracking_events":[{"status":"label_created","at":"2026-10-20T10:05:00Z","location":{"country":"DE","facility":"WH-01"}}]},{"id":"S-2","item_references":[{"line_id":"L-2","quantity":1}],"tracking_events":[{"status":"label_created","at":"2026-10-20T10:06:00Z","location":{"country":"DE","facility":"WH-02"}}]}]},"payments":[{"id":"P-1","method":{"type":"card","network":"example-network"},"status":"authorized","amount":49.8,"currency":"EUR"}]}}') AS payload
# MAGIC FROM range(10000);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE variant_inspect_unshredded USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableVariantShredding' = 'false')
# MAGIC AS SELECT * FROM variant_inspect_seed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE variant_inspect_shredded USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
# MAGIC AS SELECT * FROM variant_inspect_seed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify configuration, then look at the logical schema
# MAGIC 
# MAGIC `true` confirms the setting. It is not a field-by-field inventory or proof that a particular path was shredded. These CTAS statements wrote fresh data with explicit settings; an additional historical REORG is unnecessary.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES variant_inspect_unshredded ('delta.enableVariantShredding');

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES variant_inspect_shredded ('delta.enableVariantShredding');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE variant_inspect_shredded;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_of_variant(payload) AS logical_payload_schema
# MAGIC FROM variant_inspect_shredded
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read the same field from each representation
# MAGIC 
# MAGIC Both results should contain 10,000 rows, 10,000 usable amounts, and an average of 41.50. This checks the selected metric, not equality of every field.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'unshredded' AS layout,
# MAGIC   count(*) AS rows,
# MAGIC   count(try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)')) AS usable_amounts,
# MAGIC   avg(try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)')) AS avg_base_total
# MAGIC FROM variant_inspect_unshredded
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'shredded',
# MAGIC   count(*),
# MAGIC   count(try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)')),
# MAGIC   avg(try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)'))
# MAGIC FROM variant_inspect_shredded;

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect the reconstructed document. The result is the logical VARIANT rendered as JSON, **not a dump of the underlying binary layout**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sample_id, to_json(payload) AS reconstructed_order
# MAGIC FROM variant_inspect_shredded
# MAGIC ORDER BY sample_id
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect plans and file details
# MAGIC 
# MAGIC Compare scan/projection information in the two plans. Plan output varies by runtime and need not expose internal column names. Run the SELECT statements without EXPLAIN and inspect the query profile for actual read work. This repeated-value dataset is not designed to measure selective file skipping.

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS base_total
# MAGIC FROM variant_inspect_unshredded;

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS base_total
# MAGIC FROM variant_inspect_shredded;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL variant_inspect_unshredded;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL variant_inspect_shredded;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path AS file_path, _metadata.file_size AS file_bytes
# MAGIC FROM variant_inspect_shredded
# MAGIC ORDER BY file_path
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optional: inspect the actual physical schema with Python
# MAGIC 
# MAGIC Use an **original active `.parquet` file** from each table, not `_delta_log`, and not a newly written JSON/Parquet export of the query result. Rewriting the result could change the storage layout you are trying to inspect.
# MAGIC 
# MAGIC Unity Catalog table SELECT permission does not necessarily grant direct access to the underlying storage. Managed-table paths can prohibit path-based access. If direct access is unavailable, use an authorized external-table test location with appropriate file access, or have an authorized operator copy the exact sample file bytes to a readable Unity Catalog volume. Do not change permissions or bypass table governance for this notebook.
# MAGIC 
# MAGIC Set the paths below to original files accessible through the notebook's filesystem, for example `/Volumes/<catalog>/<schema>/<volume>/<exact-file-copy>.parquet`. Python cells need notebook compute, not a SQL-only execution environment. If PyArrow cannot parse the file's logical annotations, use a compatible PyArrow release on the test compute; that error is not evidence that shredding is absent.

# COMMAND ----------

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

# Fill with original file paths or byte-for-byte copies you are authorized to read.
PARQUET_FILES = {
    "unshredded": "",
    "shredded": "",
}

footer_tables = {}
for label, file_path in PARQUET_FILES.items():
    if not file_path:
        print(f"{label}: set PARQUET_FILES to inspect its physical storage.")
        continue

    parquet_file = pq.ParquetFile(file_path, arrow_extensions_enabled=False)
    print(f"\n{label}: original Parquet schema\n{parquet_file.schema}")

    rows = []
    for i in range(len(parquet_file.schema)):
        column = parquet_file.schema.column(i)
        rows.append({
            "physical_path": column.path,
            "physical_type": column.physical_type,
            "logical_type": str(column.logical_type),
            "inside_typed_value": "typed_value" in column.path.split("."),
        })
    footer_tables[label] = rows
    if rows:
        display(spark.createDataFrame(rows).orderBy("physical_path"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to read the output
# MAGIC 
# MAGIC Look for `metadata`, `value`, and `typed_value` under the payload's Parquet group. An illustrative nested path is `payload.typed_value.order.typed_value.total.typed_value.base_total.typed_value`; **use the path actually printed by your file**. Column mapping and writer choices can change names and layout.
# MAGIC 
# MAGIC The final `typed_value` leaf identifies typed storage for that field. A sibling `value` can hold incompatible values in encoded form. Ancestor `value` fields can retain other parts of an object. A `typed_value` path existing in the schema does not mean every row uses it.
# MAGIC 
# MAGIC The set of shredded fields depends on the written data. Arrays or additional fields may be shredded, and different files can have different layouts. Inspect more than one active file before making a claim about the whole table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optional: inspect the physical values for one observed leaf
# MAGIC 
# MAGIC Choose a full leaf path from the previous cell. This reads only the first small batch of the requested physical column, rather than reconstructing the complete logical payload. The binary fallback is encoded data, not plain JSON. NULL fallback values are normal when a compatible typed value is stored.
# MAGIC 
# MAGIC Some Arrow versions automatically expose logical extension values rather than raw groups; in that case use the footer inventory as schema evidence and a reader supporting physical-column inspection. Do not interpret an unsupported read as missing data.

# COMMAND ----------

# Set these after reviewing the actual footer, without guessing a physical path.
FILE_TO_READ = ""
PHYSICAL_LEAF = ""

if FILE_TO_READ and PHYSICAL_LEAF:
    parquet_file = pq.ParquetFile(FILE_TO_READ, arrow_extensions_enabled=False)
    known_paths = {
        parquet_file.schema.column(i).path
        for i in range(len(parquet_file.schema))
    }
    if PHYSICAL_LEAF not in known_paths:
        raise ValueError("Choose a physical leaf path printed by this file's schema.")
    batch = next(parquet_file.iter_batches(batch_size=5, columns=[PHYSICAL_LEAF]), None)
    if batch is not None:
        print(batch.to_pylist())
else:
    print("Set FILE_TO_READ and PHYSICAL_LEAF to inspect up to five stored values.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation and sources
# MAGIC 
# MAGIC - Configuration: `SHOW TBLPROPERTIES`.
# MAGIC - Logical types/values: `DESCRIBE`, `schema_of_variant`, path queries.
# MAGIC - Physical shredded fields: the original Parquet footer and stored leaves.
# MAGIC - Actual query work: query profile, not just EXPLAIN.
# MAGIC 
# MAGIC [Databricks shredding requirements and behavior](https://docs.databricks.com/aws/en/tables/features/variant-shredding) · [Parquet physical format](https://parquet.apache.org/docs/file-format/types/variantshredding/) · [PyArrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html) · [Databricks file metadata](https://docs.databricks.com/aws/en/ingestion/file-metadata-column) · [Unity Catalog path access](https://docs.databricks.com/aws/en/volumes/paths)
# MAGIC 
# MAGIC The two scratch tables remain available for inspection. No automatic cleanup runs.
