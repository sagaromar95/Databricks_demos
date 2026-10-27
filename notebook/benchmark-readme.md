# Run the comparison

Import `variant_storage_benchmark.ipynb` into Databricks as a Python notebook. The `.py` file contains the same notebook in Databricks source format.

1. Attach serverless notebook compute with VARIANT shredding support, or a supported Databricks Runtime. Select a scratch catalog and schema where you can create tables.
2. Leave `MODE = "smoke"` and run all cells. This uses 10,000 distinct generated orders, creates five uniquely named data tables, and validates the workflow. No package installation or volume is needed.
3. Download the JSON results using the link in section 9. If the link is blocked, uncomment `print(result_json)` in the next cell and copy that output.

Use the smoke run to check compatibility on the selected compute. Resolve any failed cells before increasing the row count.

## After the smoke run

Set `MODE = "measure"`, fill in `COMPUTE_NOTES`, and rerun from the configuration cell. Start with `MEASURE_ROWS = 100_000`. This creates 13 data tables: one shared source and three independently written tables per representation. It measures each write and runs five read repetitions on the last table of each format.

If scans are too small to dominate execution overhead, increase to 1,000,000 rows with the same document settings. Larger runs consume more compute and scratch storage. The notebook does not enforce a monetary budget or automatically delete its tables.

Do not run old cells against a newly changed configuration out of order. Rerunning configuration creates a fresh run ID; earlier tables stay available. If only query-history metrics are missing, rerun section 8 and then section 9 in the existing session, without rerunning configuration.

## Experiments to keep separate

| Experiment | Change | Keep unchanged |
| --- | --- | --- |
| Scale | Row count | Document shape and compute |
| Width | `EXTRA_FIELDS`, e.g. 0 then 20 | Row count, array limits and compute |
| Item arrays | `MAX_ITEMS`, e.g. 4 then 12 | Row count, width, tracking history and compute |
| Tracking arrays | `MAX_TRACKING_EVENTS`, e.g. 3 then 12 | Row count, width, items and compute |

The generator uses deterministic hashes to vary prices, quantities, IDs, categories, and array lengths. It keeps the ecommerce structure, but is synthetic data rather than a captured production distribution. The same settings produce the same logical input.

## What the results mean

- **Storage:** active table data bytes and file counts. Does not include all historical storage, logs, backups, shared source retention, or cloud billing.
- **Writes:** JSON-to-table client elapsed time, excluding source generation and table creation. Includes reading the source, applicable parsing, encoding, compression, writing, and commit work. Shuffled order reduces a fixed ordering advantage; it does not eliminate all serverless variability.
- **Reads:** narrow aggregation, filtered aggregation, an item-array aggregation, and whole-document JSON output. Read timings follow full-document validation and workload warm-ups; they are not cold-cache benchmarks.
- **Correctness:** every document is compared after normalization to the complete declared schema, then each analytical query's results are compared across formats. This does not test unknown fields, type drift, original JSON spelling, or absent-versus-explicit-null semantics.
- **Whole-document output:** reads and consumes the payload by summing serialized JSON lengths. STRING already holds JSON; STRUCT and VARIANT must serialize it. Label this serialization workload separately from narrow analytical reads. JSON lengths need not match across representations.
- **Engine metrics:** optional `system.query.history` lookup, subject to permissions and delivery delay. Exclude result-cache hits and failed statements. Inspect queue/compute waits and I/O-cache percentages. An empty lookup leaves engine metrics unavailable.
- **Cost:** elapsed time is not DBUs or currency. Compute/billing evidence and storage pricing would be needed for monetary claims; neither is estimated here.

All table names include a unique run ID. Both VARIANT settings are explicit and verified. A setting enables the writer; the included EXPLAIN plans help check whether the amount query actually benefits from a typed read. The notebook does not claim to inventory the physical shredding of every field.

## Results export

The downloaded JSON contains configuration, raw repetitions, storage measurements, validation results, query plans, exact queries, and available engine metrics. Record the compute settings alongside it. Query profiles provide additional context for execution time, bytes read, file pruning, and cache use.

The SQL warehouse scripts in `sql-warehouse/` measure reads separately using query-history execution metrics. Keep these separate from the notebook’s client elapsed timings.
