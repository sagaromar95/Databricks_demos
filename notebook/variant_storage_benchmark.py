# Databricks notebook source
# MAGIC %md
# MAGIC # Same orders, four storage representations
# MAGIC Compare JSON STRING, a full persisted STRUCT, unshredded VARIANT, and shredded VARIANT.
# MAGIC
# MAGIC Start with the default smoke run to check compatibility and correctness. Use Databricks notebook compute with VARIANT shredding support. The small smoke dataset is for validation, not performance comparisons.
# MAGIC
# MAGIC Each run creates uniquely named scratch tables in your current catalog/schema. No existing tables are replaced or deleted. No volume, PyArrow, package installation, RDD API, or manual DataFrame cache is needed. You need permission to create tables.
# MAGIC
# MAGIC The measured write is **JSON-to-table ingestion**, including source reads, parsing where applicable, encoding, compression, and the Delta commit. Reads use small aggregate results, so the client does not collect millions of documents. Timings from Python are **client elapsed seconds**, not engine execution time or money. The optional query-history cell adds engine metrics when accessible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Run in a scratch schema selected in the notebook UI. Change `MODE` to `measure` only after the smoke run succeeds. Start at 100,000 rows, then increase `MEASURE_ROWS` if query times are dominated by overhead. Compare separate runs with one changed parameter at a time.
# MAGIC
# MAGIC - Row-count experiment: change `MEASURE_ROWS` only.
# MAGIC - Array-complexity experiment: change `MAX_ITEMS` or `MAX_TRACKING_EVENTS`, not both.
# MAGIC - Width experiment: change `EXTRA_FIELDS` only.
# MAGIC - Type drift and schema evolution belong in a separate correctness experiment. This dataset has compatible types; STRUCT must retain every generated field.

# COMMAND ----------

import base64
import html
import json
import random
import statistics
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DecimalType, LongType, StringType, StructField, StructType,
)

MODE = "smoke"                         # smoke or measure
MEASURE_ROWS = 100_000                  # Increase row count without changing the payload shape
MAX_ITEMS = 4
MAX_TRACKING_EVENTS = 3
EXTRA_FIELDS = 0                       # Additional STRING fields per document
COMPUTE_NOTES = "Fill in compute type, environment/runtime, size if fixed, Photon, and concurrency."

assert MODE in {"smoke", "measure"}
assert MEASURE_ROWS > 0 and 1 <= MAX_ITEMS <= 100
assert 1 <= MAX_TRACKING_EVENTS <= 100 and 0 <= EXTRA_FIELDS <= 100
N_ROWS = 10_000 if MODE == "smoke" else MEASURE_ROWS
WRITE_TRIALS = 1 if MODE == "smoke" else 3
READ_REPEATS = 2 if MODE == "smoke" else 5
FORMATS = ("string", "struct", "variant", "shredded")
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
context = spark.sql("SELECT current_catalog() AS c, current_schema() AS s").first()

def ident(name):
    return "`" + name.replace("`", "``") + "`"

def literal(value):
    return "'" + value.replace("'", "''") + "'"

def table_name(suffix):
    return ".".join(ident(x) for x in (context.c, context.s, f"vb_{RUN_ID}_{suffix}"))

SOURCE = table_name("source")
TABLES = {(fmt, trial): table_name(f"{fmt}_{trial}")
          for trial in range(WRITE_TRIALS) for fmt in FORMATS}
run_config = {
    "run_id": RUN_ID, "mode": MODE, "rows": N_ROWS,
    "max_items": MAX_ITEMS, "max_tracking_events": MAX_TRACKING_EVENTS,
    "extra_fields": EXTRA_FIELDS, "write_trials": WRITE_TRIALS,
    "read_repeats": READ_REPEATS, "compute_notes": COMPUTE_NOTES,
    "spark_version": spark.version, "catalog": context.c, "schema": context.s,
    "timing": "client_elapsed_seconds", "read_condition": "after validation and warm-up",
}
print(json.dumps(run_config, indent=2))
print(f"Will create {1 + len(TABLES)} new data tables. No automatic cleanup.")

# Result-cache settings vary by compute. A failed SET does not prove that caching is off.
try:
    spark.sql("SET use_cached_result = false").collect()
    run_config["result_cache_setting"] = "SET accepted; verify actual query-history cache flags"
except Exception as exc:
    run_config["result_cache_setting"] = "SET unavailable; check result-cache flags in query profiles/history"
    print("Result-cache setting unavailable:", str(exc)[:300])
print(run_config["result_cache_setting"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full STRUCT schema
# MAGIC Every generated field is represented, including arrays and optional product attributes. Missing JSON attributes become nullable fields in STRUCT; original whitespace, key order, and absent-versus-explicit-null are not preserved by STRUCT. Those lexical distinctions are outside this benchmark. All generated numbers have compatible fixed types.

# COMMAND ----------

def S(*fields):
    return StructType([StructField(name, dtype, True) for name, dtype in fields])

text_type, integer, money = StringType(), LongType(), DecimalType(18, 2)
detail_schema = S(("type", text_type), ("rate_pct", money), ("amount", money))
breakdown_schema = S(("type", text_type), ("amount", money),
                     ("detailed_breakdowns", ArrayType(detail_schema)))
attributes_schema = S(
    ("connectivity", ArrayType(text_type)),
    ("compatibility", S(("operating_systems", ArrayType(text_type)))),
    ("size", text_type), ("material", S(("cotton_pct", integer))),
    ("care_instructions", ArrayType(text_type)),
)
item_schema = S(
    ("line_id", text_type),
    ("product", S(("sku", text_type), ("category", text_type), ("attributes", attributes_schema))),
    ("quantity", integer),
    ("pricing", S(("unit_price", money), ("breakdowns", ArrayType(breakdown_schema)))),
)
tracking_schema = S(("status", text_type), ("at", text_type),
                    ("location", S(("country", text_type), ("facility", text_type))))
shipment_schema = S(
    ("id", text_type),
    ("item_references", ArrayType(S(("line_id", text_type), ("quantity", integer)))),
    ("tracking_events", ArrayType(tracking_schema)),
)
payment_schema = S(
    ("id", text_type), ("method", S(("type", text_type), ("network", text_type))),
    ("status", text_type), ("amount", money), ("currency", text_type),
)
order_fields = [
    ("id", text_type), ("currency", text_type),
    ("customer", S(("id", text_type), ("tier", text_type))),
    ("total", S(("base_total", money), ("tax_pct", money), ("tax_amount", money))),
    ("items", ArrayType(item_schema)),
    ("fulfillment", S(("shipments", ArrayType(shipment_schema)))),
    ("payments", ArrayType(payment_schema)),
]
if EXTRA_FIELDS:
    order_fields.append(("extra_attributes", S(*[
        (f"attribute_{i:03d}", text_type) for i in range(EXTRA_FIELDS)
    ])))
PAYLOAD_SCHEMA = S(
    ("event_id", text_type), ("schema_version", integer), ("event_type", text_type),
    ("occurred_at", text_type), ("order", S(*order_fields)),
)
SCHEMA_SQL = literal(PAYLOAD_SCHEMA.simpleString())
print(PAYLOAD_SCHEMA.simpleString())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate varied orders once, outside all measured writes
# MAGIC Uses native Spark expressions, not a Python UDF. Prices, IDs, categories, quantities, array lengths, and optional attributes vary deterministically. Totals are derived from line amounts; payment equals base total plus tax. Repeating runs with the same settings reproduces the logical input.
# MAGIC
# MAGIC This is synthetic data with bounded cardinalities, not an Amazon dataset or a universal compression model. Source generation is not included in ingestion timing. All four representations read this same materialized JSON source.

# COMMAND ----------

extra_sql = ""
if EXTRA_FIELDS:
    entries = ", ".join(
        f"'attribute_{i:03d}', substring(sha2(concat(cast(id AS STRING), '-{i}'), 256), 1, 24)"
        for i in range(EXTRA_FIELDS)
    )
    extra_sql = f", 'extra_attributes', named_struct({entries})"

generation_sql = f"""
CREATE TABLE {SOURCE} USING DELTA AS
WITH seeds AS (
  SELECT id,
    element_at(array('EUR', 'USD', 'GBP'), cast(pmod(xxhash64(id, 17), 3) + 1 AS INT)) AS currency,
    element_at(array('gold', 'silver', 'bronze'), cast(pmod(xxhash64(id, 29), 3) + 1 AS INT)) AS tier,
    transform(sequence(1, cast(pmod(xxhash64(id, 31), {MAX_ITEMS}) + 1 AS INT)), k ->
      named_struct('k', k,
        'quantity', cast(pmod(xxhash64(id, k, 41), 3) + 1 AS BIGINT),
        'unit_price', cast((pmod(xxhash64(id, k, 53), 10000) + 100) / 100.0 AS DECIMAL(18,2))
      )
    ) AS lines
  FROM range({N_ROWS})
), priced AS (
  SELECT *, transform(lines, x -> named_struct(
    'line_id', concat('L-', cast(x.k AS STRING)),
    'product', named_struct(
      'sku', concat('SKU-', cast(pmod(xxhash64(id, x.k, 71), 10000) AS STRING)),
      'category', CASE WHEN pmod(x.k, 2) = 1 THEN 'electronics' ELSE 'clothing' END,
      'attributes', named_struct(
        'connectivity', CASE WHEN pmod(x.k, 2) = 1 THEN array('bluetooth', 'usb') END,
        'compatibility', CASE WHEN pmod(x.k, 2) = 1
          THEN named_struct('operating_systems', array('Windows', 'macOS')) END,
        'size', CASE WHEN pmod(x.k, 2) = 0
          THEN element_at(array('S', 'M', 'L'), cast(pmod(id + x.k, 3) + 1 AS INT)) END,
        'material', CASE WHEN pmod(x.k, 2) = 0 THEN named_struct('cotton_pct', cast(100 AS BIGINT)) END,
        'care_instructions', CASE WHEN pmod(x.k, 2) = 0 THEN array('wash_cold', 'line_dry') END
      )
    ),
    'quantity', x.quantity,
    'pricing', named_struct('unit_price', x.unit_price,
      'breakdowns', array(named_struct('type', 'tax',
        'amount', cast(round(x.unit_price * x.quantity * 0.20, 2) AS DECIMAL(18,2)),
        'detailed_breakdowns', array(named_struct('type', 'VAT',
          'rate_pct', cast(20 AS DECIMAL(18,2)),
          'amount', cast(round(x.unit_price * x.quantity * 0.20, 2) AS DECIMAL(18,2))
        ))
      ))
    )
  )) AS items FROM seeds
), totals AS (
  SELECT *,
    aggregate(lines, cast(0 AS DECIMAL(18,2)),
      (acc, x) -> cast(acc + x.unit_price * x.quantity AS DECIMAL(18,2))) AS base_total,
    aggregate(items, cast(0 AS DECIMAL(18,2)),
      (acc, x) -> cast(acc + x.pricing.breakdowns[0].amount AS DECIMAL(18,2))) AS tax_amount
  FROM priced
)
SELECT id AS sample_id, to_json(named_struct(
  'event_id', concat('evt-', cast(id AS STRING)),
  'schema_version', cast(1 AS BIGINT), 'event_type', 'order_placed',
  'occurred_at', concat('2026-10-', lpad(cast(pmod(id, 28) + 1 AS STRING), 2, '0'), 'T10:00:00Z'),
  'order', named_struct(
    'id', concat('A-', cast(id AS STRING)), 'currency', currency,
    'customer', named_struct('id', concat('C-', cast(pmod(id, 20000) AS STRING)), 'tier', tier),
    'total', named_struct('base_total', base_total,
      'tax_pct', cast(20 AS DECIMAL(18,2)), 'tax_amount', tax_amount),
    'items', items,
    'fulfillment', named_struct('shipments', array(named_struct(
      'id', concat('S-', cast(id AS STRING)),
      'item_references', transform(lines, x -> named_struct(
        'line_id', concat('L-', cast(x.k AS STRING)), 'quantity', x.quantity)),
      'tracking_events', transform(
        sequence(1, cast(pmod(xxhash64(id, 83), {MAX_TRACKING_EVENTS}) + 1 AS INT)), k ->
        named_struct('status', CASE WHEN k = 1 THEN 'label_created' ELSE 'in_transit' END,
          'at', concat('2026-10-29T', lpad(cast(k % 24 AS STRING), 2, '0'), ':00:00Z'),
          'location', named_struct('country', 'DE',
            'facility', concat('WH-', cast(pmod(id + k, 50) AS STRING))))
      )
    ))),
    'payments', array(named_struct('id', concat('P-', cast(id AS STRING)),
      'method', named_struct('type', 'card', 'network', 'example-network'),
      'status', 'authorized', 'amount', cast(base_total + tax_amount AS DECIMAL(18,2)),
      'currency', currency))
    {extra_sql}
  )
), map('ignoreNullFields', 'true')) AS raw_json
FROM totals
"""
spark.sql(generation_sql).collect()

# Materialize and read the whole source before measuring ingestion, without cache APIs.
source_stats = spark.sql(f"""
SELECT count(*) AS rows, sum(octet_length(raw_json)) AS uncompressed_json_bytes,
       min(octet_length(raw_json)) AS min_json_bytes,
       max(octet_length(raw_json)) AS max_json_bytes
FROM {SOURCE}
""").first().asDict()
assert source_stats["rows"] == N_ROWS
run_config["source_stats"] = source_stats
display(spark.table(SOURCE).limit(3))
print(source_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Measure writes and active storage
# MAGIC Every trial writes a fresh table. Format order is shuffled reproducibly per trial. Table creation/registration is outside the timer; the INSERT is timed through completion. No OPTIMIZE, clustering, or historical rewrite is run. Engine-managed file sizing and statistics remain part of the measured system; file counts and table properties are recorded.
# MAGIC
# MAGIC `sizeInBytes` is the latest table snapshot's data size. It is not a bill for all retained historical files, logs, backups, or the shared source. All representations contain `sample_id` plus one payload; none includes an extra raw copy.

# COMMAND ----------

write_results, storage_results, query_manifest, read_results = [], [], [], []
SQL_BY_KEY = {}

def tagged_sql(sql, key):
    # Comments aid lookup; they are NOT used as a cache-bypass technique.
    return f"/* variant_benchmark {RUN_ID} {key} */\n{sql}"

def timed_sql(sql, key, kind, **labels):
    text = tagged_sql(sql, key)
    started = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()
    rows = spark.sql(text).collect()
    elapsed = time.perf_counter() - t0
    entry = {"key": key, "kind": kind, "started_utc": started,
             "client_elapsed_s": elapsed, **labels}
    query_manifest.append({**entry, "sql": text})
    SQL_BY_KEY[key] = text
    return rows, entry

for trial in range(WRITE_TRIALS):
    order = list(FORMATS)
    random.Random(1700 + trial).shuffle(order)
    for fmt in order:
        table = TABLES[fmt, trial]
        dtype = {"string": "STRING", "struct": PAYLOAD_SCHEMA.simpleString(),
                 "variant": "VARIANT", "shredded": "VARIANT"}[fmt]
        prop = ""
        if fmt in {"variant", "shredded"}:
            enabled = "true" if fmt == "shredded" else "false"
            prop = f"TBLPROPERTIES ('delta.enableVariantShredding' = '{enabled}')"
        spark.sql(f"CREATE TABLE {table} (sample_id BIGINT, payload {dtype}) USING DELTA {prop}").collect()
        expression = {
            "string": "raw_json",
            "struct": f"from_json(raw_json, {SCHEMA_SQL}, map('mode', 'FAILFAST'))",
            "variant": "parse_json(raw_json)", "shredded": "parse_json(raw_json)",
        }[fmt]
        sql = f"INSERT INTO {table} SELECT sample_id, {expression} FROM {SOURCE}"
        _, result = timed_sql(sql, f"write_{fmt}_{trial}", "write", format=fmt, trial=trial, table=table)
        result["rows_per_second"] = N_ROWS / result["client_elapsed_s"]
        write_results.append(result)
        info = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)
        if fmt in {"variant", "shredded"}:
            actual = str(info["properties"].get("delta.enableVariantShredding", "")).lower()
            assert actual == enabled, (fmt, actual, enabled)
        storage_results.append({
            "format": fmt, "trial": trial, "table": table,
            "active_bytes": int(info["sizeInBytes"]), "files": int(info["numFiles"]),
            "bytes_per_row": int(info["sizeInBytes"]) / N_ROWS,
            "properties": info.get("properties", {}),
        })
        print(f"{fmt}, trial {trial}: {result['client_elapsed_s']:.2f}s, "
              f"{info['sizeInBytes'] / 1024**2:.2f} MiB, {info['numFiles']} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify complete documents, not just one amount
# MAGIC Compare every row against the source after parsing both into the same complete schema. This checks nested arrays, money, optional fields and added width fields; it does not check the original JSON spelling or missing-versus-explicit-null semantics. The generated input contains no unknown schema fields. Validation reads and warms the tables, so later measurements are **not cold reads**.

# COMMAND ----------

validation_results = []
for (fmt, trial), table in TABLES.items():
    document = "t.payload" if fmt == "struct" else (
        f"from_json(t.payload, {SCHEMA_SQL}, map('mode', 'FAILFAST'))" if fmt == "string" else
        f"from_json(to_json(t.payload), {SCHEMA_SQL}, map('mode', 'FAILFAST'))"
    )
    validation = spark.sql(f"""
      SELECT count(*) AS compared_rows,
        sum(CASE WHEN s.sample_id IS NULL OR t.sample_id IS NULL OR NOT (
          {document} <=> from_json(s.raw_json, {SCHEMA_SQL}, map('mode', 'FAILFAST'))
        ) THEN 1 ELSE 0 END) AS mismatched_rows
      FROM {SOURCE} s FULL OUTER JOIN {table} t ON s.sample_id = t.sample_id
    """).first().asDict()
    assert validation["compared_rows"] == N_ROWS, (fmt, trial, validation)
    assert validation["mismatched_rows"] == 0, (fmt, trial, validation)
    validation_results.append({"format": fmt, "trial": trial, **validation})
print("Complete-document equality passed for every representation and write trial.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Four read workloads
# MAGIC Narrow and filtered aggregates use identical output types. The STRING baseline uses one `from_json` projection, giving the optimizer an opportunity to prune parsing work rather than deliberately extracting each field repeatedly. The array query expands items and aggregates by category.
# MAGIC
# MAGIC `whole_document_output` forces payload access and sums JSON output lengths without transferring all documents to the client. STRING already holds text; STRUCT and VARIANT serialize to text. This is a **whole-document serialization workload**, not pure scan performance. Output lengths may differ because JSON formatting and field order are representation-dependent; only row counts must match for this workload.

# COMMAND ----------

REPORT_SCHEMA = literal("STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>")
ITEM_REPORT_SCHEMA = literal("STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>")

def queries(fmt, table):
    if fmt == "string":
        projection = f"""SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, {REPORT_SCHEMA}, map('mode', 'FAILFAST')) AS p FROM {table})"""
        array_from = f"""{table} t, LATERAL explode(
            from_json(t.payload, {ITEM_REPORT_SCHEMA}, map('mode', 'FAILFAST')).order.items
        ) AS e(item)"""
        category, qty, output = "e.item.product.category", "e.item.quantity", "payload"
    elif fmt == "struct":
        projection = f"""SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM {table}"""
        array_from = f"{table} t, LATERAL explode(t.payload.order.items) AS e(item)"
        category, qty, output = "e.item.product.category", "e.item.quantity", "to_json(payload)"
    else:
        projection = f"""SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM {table}"""
        array_from = f"{table} t, LATERAL variant_explode(t.payload:order.items) AS e"
        category = "try_variant_get(e.value, '$.product.category', 'string')"
        qty = "try_variant_get(e.value, '$.quantity', 'bigint')"
        output = "to_json(payload)"
    return {
        "narrow": f"""WITH p AS ({projection}) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency""",
        "filtered": f"""WITH p AS ({projection}) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency""",
        "items": f"SELECT {category} AS category, count(*) AS n, sum({qty}) AS quantity FROM {array_from} GROUP BY 1 ORDER BY 1",
        "whole_document_output": f"SELECT count(*) AS n, sum(octet_length({output})) AS output_bytes FROM {table}",
    }

def stable_result(rows):
    return json.dumps([row.asDict(recursive=True) for row in rows], default=str, sort_keys=True)

# A separate query warm-up on the last trial's table; all trials were already validated.
READ_TRIAL = WRITE_TRIALS - 1
expected = {}
for fmt in FORMATS:
    for workload, sql in queries(fmt, TABLES[fmt, READ_TRIAL]).items():
        rows, _ = timed_sql(sql, f"warmup_{fmt}_{workload}", "warmup", format=fmt, workload=workload)
        if workload == "whole_document_output":
            assert rows[0]["n"] == N_ROWS
        elif workload not in expected:
            expected[workload] = stable_result(rows)
        else:
            assert stable_result(rows) == expected[workload], (fmt, workload, rows)
print("Workload results agree. Warm-ups are excluded from summaries.")

# COMMAND ----------

for repeat in range(READ_REPEATS):
    tasks = [(fmt, name, sql) for fmt in FORMATS
             for name, sql in queries(fmt, TABLES[fmt, READ_TRIAL]).items()]
    random.Random(2900 + repeat).shuffle(tasks)
    for fmt, workload, sql in tasks:
        rows, result = timed_sql(sql, f"read_{fmt}_{workload}_{repeat}", "read",
                                 format=fmt, workload=workload, repeat=repeat)
        if workload == "whole_document_output":
            assert rows[0]["n"] == N_ROWS
        else:
            assert stable_result(rows) == expected[workload], (fmt, workload)
        read_results.append(result)
    print(f"Read round {repeat + 1}/{READ_REPEATS} complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Results and plans
# MAGIC A smoke run is a functionality check. Even measure-mode elapsed times remain provisional until result-cache flags, compute settings, and query profiles are reviewed. Small datasets can be dominated by scheduling and compilation. Min/max show observed spread; they are not confidence intervals.

# COMMAND ----------

summary = []
for fmt in FORMATS:
    writes = [r["client_elapsed_s"] for r in write_results if r["format"] == fmt]
    sizes = [r["active_bytes"] for r in storage_results if r["format"] == fmt]
    summary.append((fmt, "write_json_to_table", statistics.median(writes), min(writes), max(writes),
                    statistics.median(sizes) / 1024**2, statistics.median(sizes) / N_ROWS))
    for workload in queries(fmt, TABLES[fmt, READ_TRIAL]):
        seconds = [r["client_elapsed_s"] for r in read_results
                   if r["format"] == fmt and r["workload"] == workload]
        summary.append((fmt, workload, statistics.median(seconds), min(seconds), max(seconds),
                        statistics.median(sizes) / 1024**2, statistics.median(sizes) / N_ROWS))
summary_df = spark.createDataFrame(summary,
    "format string, workload string, median_client_s double, min_client_s double, max_client_s double, active_MiB double, bytes_per_row double")
display(summary_df.orderBy("workload", "format"))

plans = {}
for fmt in ("variant", "shredded"):
    table = TABLES[fmt, READ_TRIAL]
    plan = "\n".join(row[0] for row in spark.sql(f"""EXPLAIN FORMATTED
      SELECT try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS base_total
      FROM {table}
    """).collect())
    plans[fmt] = plan
    print(f"\n{fmt.upper()}\n{plan}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optional engine metrics (may arrive later)
# MAGIC If you can read `system.query.history`, this cell looks up tagged statements. Records can take about an hour to arrive. Lack of access or missing rows does not invalidate the data checks, but leaves engine-level performance evidence incomplete. Rerun this cell and the export cell later if needed.
# MAGIC
# MAGIC For execution-time comparisons, exclude `from_result_cache = true` and failed statements; separate queue/compute startup from execution. Inspect `read_io_cache_percent` alongside `read_bytes`: the latter mixes compressed remote reads and uncompressed cache reads, so it is not a storage compression ratio. `read_files_bytes` is the size of files selected, not necessarily the bytes of columns consumed. Do not convert client seconds or task milliseconds directly into DBUs or currency.

# COMMAND ----------

engine_metrics = []
engine_metrics_status = "not attempted"
try:
    history = spark.table("system.query.history")
    available = set(history.columns)
    wanted = ["statement_id", "statement_text", "execution_status", "start_time", "end_time",
              "execution_duration_ms", "compilation_duration_ms", "total_duration_ms",
              "waiting_for_compute_duration_ms", "waiting_at_capacity_duration_ms",
              "total_task_duration_ms", "read_bytes", "read_files", "pruned_files",
              "read_files_bytes", "pruned_files_bytes", "read_io_cache_percent",
              "from_result_cache", "written_bytes", "written_rows", "written_files"]
    metrics_df = history.where(
        F.col("start_time") >= F.to_timestamp(F.lit(RUN_ID[:8]), "yyyyMMdd")
    ).where(F.col("statement_text").contains(f"/* variant_benchmark {RUN_ID} "))
    metrics_df = metrics_df.select(*[c for c in wanted if c in available])
    candidates = [r.asDict(recursive=True) for r in metrics_df.collect()]
    for row in candidates:
        # Ignore queries that merely contain the lookup string instead of our execution tag.
        statement = row.get("statement_text", "") or ""
        if not statement.lstrip().startswith(f"/* variant_benchmark {RUN_ID} "):
            continue
        row["key"] = statement.split(" */", 1)[0].split()[-1]
        engine_metrics.append(row)
    engine_metrics_status = f"Retrieved {len(engine_metrics)} matching statement records; expected at least {len(query_manifest)} including warm-ups."
    print(engine_metrics_status)
    display(metrics_df)
except Exception as exc:
    engine_metrics_status = "Unavailable: " + str(exc)[:500]
    print(engine_metrics_status)
    print("Use Query History / query profiles to record execution time, cache hits, bytes read and files pruned.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Download the evidence bundle
# MAGIC Download the JSON results to retain the configuration and measurements for this run. It includes config, raw repetitions, storage, validation, plans, exact queries, and any available engine metrics. There are no credentials or source order documents in the bundle, but it does contain scratch table names and statement IDs. If the download link is blocked by your workspace, copy the printed JSON from the final cell instead.
# MAGIC
# MAGIC Keep the scratch tables until the measurements and plans have been reviewed. This notebook does not run DROP, VACUUM, REORG or OPTIMIZE. To remove them later, use the exact run-specific inventory printed below.

# COMMAND ----------

evidence = {
    "config": run_config, "source_table": SOURCE,
    "tables": [{"format": fmt, "trial": trial, "table": name} for (fmt, trial), name in TABLES.items()],
    "validation": validation_results, "writes": write_results, "storage": storage_results,
    "reads": read_results, "queries": query_manifest, "plans": plans,
    "engine_metrics_status": engine_metrics_status, "engine_metrics": engine_metrics,
    "publication_status": "PROVISIONAL: cache flags, compute settings, dataset size and query profiles require verification",
}
result_json = json.dumps(evidence, default=str, indent=2)
encoded = base64.b64encode(result_json.encode()).decode()
filename = f"variant_benchmark_{RUN_ID}.json"
displayHTML(f'<a download="{html.escape(filename)}" href="data:application/json;base64,{encoded}">Download benchmark results</a>')
print("Created tables for this run:")
print("\n".join([SOURCE, *TABLES.values()]))

# COMMAND ----------

# Uncomment only if the download link does not work:
# print(result_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## References and interpretation
# MAGIC - [Databricks VARIANT shredding](https://docs.databricks.com/aws/en/tables/features/variant-shredding): compatibility, explicit table controls and additional write work.
# MAGIC - [Table details](https://docs.databricks.com/aws/en/delta/table-details): snapshot sizes and file counts.
# MAGIC - [Query history system table](https://docs.databricks.com/aws/en/admin/system-tables/query-history): metric definitions, cache flags, permissions and delivery latency.
# MAGIC - [Query caching](https://docs.databricks.com/aws/en/sql/user/queries/query-caching): result caching differs from disk caching.
# MAGIC - [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations): no manual DataFrame/SQL cache APIs.
# MAGIC
# MAGIC This experiment compares complete, compatible documents. It does not measure updates/deletes, rewrite/backfill expense, schema-maintenance labor, adversarial type drift, or monetary cost. Those require separate experiments. A faster result is not evidence that every workload will improve or that billing falls by the same percentage.
