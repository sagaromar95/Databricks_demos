-- Run after the benchmark records become available in system.query.history.
SELECT
    statement_id,
    workspace_id,
    session_id,
    start_time,
    compute.warehouse_id AS warehouse_id,
    regexp_extract(statement_text, 'vb_items_20261028_171826 (warmup|measure)', 1) AS phase,
    regexp_extract(statement_text, 'vb_items_20261028_171826 (?:warmup|measure) ([a-z_]+)', 1) AS query_pattern,
    CAST(regexp_extract(statement_text, 'vb_items_20261028_171826 (?:warmup|measure) [a-z_]+ ([0-9]+)', 1) AS INT) AS repetition,
    execution_status,
    execution_duration_ms,
    total_duration_ms,
    compilation_duration_ms,
    waiting_for_compute_duration_ms,
    waiting_at_capacity_duration_ms,
    from_result_cache,
    read_io_cache_percent,
    read_bytes,
    read_rows,
    read_files,
    pruned_files,
    shuffle_read_bytes,
    spilled_local_bytes
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND ltrim(statement_text) LIKE '/* vb_items_20261028_171826 %'
ORDER BY start_time;
