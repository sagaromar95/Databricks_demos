-- Run after the benchmark; export the result as CSV with all rows.
-- Query-history delivery is typically within one hour; new workspaces can take longer.
-- Requires access to system.query.history and unmasked statement text for tag matching.
-- If no rows match, inspect Query History using saved query IDs; zero matches do NOT mean no cache.
-- This includes failures, cache hits, and warm-ups so they can be audited, not silently discarded.
-- Expected: 16 warm-ups and 80 measured observations, one warehouse, 5 per format/workload.
-- If rerunning the benchmark file, change its run tag and this lookup to keep runs separate.
SET use_cached_result = false;
WITH observations AS (
  SELECT
    regexp_extract(statement_text, 'vb_wh_1m_d10457b0 (warmup|measure) ([a-z_]+) ([a-z]+) ([0-9]+)', 1) AS phase,
    regexp_extract(statement_text, 'vb_wh_1m_d10457b0 (warmup|measure) ([a-z_]+) ([a-z]+) ([0-9]+)', 2) AS workload,
    regexp_extract(statement_text, 'vb_wh_1m_d10457b0 (warmup|measure) ([a-z_]+) ([a-z]+) ([0-9]+)', 3) AS representation,
    try_cast(regexp_extract(statement_text, 'vb_wh_1m_d10457b0 (warmup|measure) ([a-z_]+) ([a-z]+) ([0-9]+)', 4) AS INT) AS repetition,
    statement_id, session_id, compute.warehouse_id AS warehouse_id,
    start_time, end_time, execution_status,
    from_result_cache, cache_origin_statement_id,
    execution_duration_ms, total_duration_ms, compilation_duration_ms,
    waiting_for_compute_duration_ms, waiting_at_capacity_duration_ms,
    result_fetch_duration_ms, read_rows, read_bytes, read_files,
    pruned_files, read_io_cache_percent, statement_text
  FROM system.query.history
  WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
    AND executed_by = current_user()
    AND contains(statement_text, 'vb_wh_1m_d10457b0')
)
SELECT * FROM observations
WHERE phase IN ('warmup', 'measure')
ORDER BY start_time, statement_id;
