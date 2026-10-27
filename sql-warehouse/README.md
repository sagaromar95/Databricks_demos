# SQL warehouse read comparison — 1 million rows

1. Select a 2X-Small SQL warehouse. Record its ID, type, channel, and concurrency context.
2. Open `variant_1m_warehouse_reads.sql` in the SQL editor. Run the preflight first; all four counts must be 1,000,000. These are the existing `_2` tables, not the `_source` input table.
3. Execute the remaining statements sequentially. Stop on an error. Each benchmark SELECT is preceded by `SET use_cached_result = false`; when selecting statements, include that SET in the same execution/session. Do not change queries between warm-up and measurement.
4. Keep each query ID/profile and verify the three analytical workloads return matching results across formats. The JSON-output-size workload measures aggregate serialized length, not document transfer; its byte totals need not match.
5. After query history has arrived, run `variant_1m_warehouse_metrics.sql` and export all results as CSV. Expected: 96 observations (16 warm-ups, 80 measured), one warehouse, five measured repetitions for every format/workload. Resolve missing or duplicate observations before calculating medians. Check successful execution, result-cache indicators, queue times, and IO-cache percentages. Exclude result-cache hits from performance comparisons; obtain replacement observations if necessary.
6. If history is unavailable or statement text is masked/rewritten, use the saved query IDs and Query History profiles. Do not interpret a failed lookup as evidence that caching was disabled.

These scripts measure reads on existing tables. Storage and write measurements come from the notebook. Warehouse engine execution time and notebook client elapsed time measure different work and should be reported separately. Automatic data caching remains enabled; use the recorded I/O-cache percentages to describe the read conditions.

The scripts query existing tables without modifying their data. Before repeating the whole experiment, use a fresh run tag in both SQL files so observations are not mixed.

Sources: [result cache](https://docs.databricks.com/aws/en/sql/user/queries/query-caching), [query history](https://docs.databricks.com/aws/en/admin/system-tables/query-history).
