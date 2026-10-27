-- 1M-row read benchmark on a 2X-Small SQL warehouse.
-- Run tag: vb_wh_1m_d10457b0
-- Existing tables from 20261023_193651_c739a157, write trial 2.
-- The _source table contains raw_json; do NOT substitute it for these payload tables.
-- No tables are created, changed, or deleted.
-- Record warehouse ID, serverless/pro/classic type, 2X-Small size,
-- channel, start/end time, and concurrent workloads before running.
-- Execute statements sequentially in one SQL editor session, not in parallel tabs.
-- Stop if SET fails or any required table is missing.
-- SET is repeated before every benchmark SELECT for clarity when running selections.
-- Select SET and the following SELECT together if running a query individually.
-- Comments identify observations; they do not bypass caching.
-- 16 untimed warm-ups, followed by 80 measured queries (5 x 4 workloads x 4 formats).
-- Keep query IDs/profiles. Compare analytical outputs across formats.
-- JSON-output byte totals need not match: JSON serialization can change spelling.

SET use_cached_result = false;

-- Preflight: each existing representation should contain 1,000,000 rows.
SELECT 'string' AS representation, count(*) AS row_count FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`
UNION ALL
SELECT 'struct' AS representation, count(*) AS row_count FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`
UNION ALL
SELECT 'variant' AS representation, count(*) AS row_count FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`
UNION ALL
SELECT 'shredded' AS representation, count(*) AS row_count FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- WARMUP | narrow | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup narrow string 1 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | narrow | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup narrow struct 1 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | narrow | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup narrow variant 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | narrow | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup narrow shredded 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | filtered | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup filtered struct 1 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | filtered | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup filtered variant 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | filtered | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup filtered shredded 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | filtered | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup filtered string 1 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- WARMUP | items | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup items variant 1 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- WARMUP | items | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup items shredded 1 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- WARMUP | items | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup items string 1 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2` t, LATERAL explode(
            from_json(t.payload, 'STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>', map('mode', 'FAILFAST')).order.items
        ) AS e(item) GROUP BY 1 ORDER BY 1;

-- WARMUP | items | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup items struct 1 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

-- WARMUP | whole_document_output | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup whole_document_output shredded 1 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- WARMUP | whole_document_output | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup whole_document_output string 1 */
SELECT count(*) AS n, sum(octet_length(payload)) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`;

-- WARMUP | whole_document_output | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup whole_document_output struct 1 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`;

-- WARMUP | whole_document_output | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 warmup whole_document_output variant 1 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`;

-- MEASURE | narrow | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow string 1 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow struct 1 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow variant 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow shredded 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered struct 1 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered variant 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered shredded 1 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered string 1 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | items | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items variant 1 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items shredded 1 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items string 1 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2` t, LATERAL explode(
            from_json(t.payload, 'STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>', map('mode', 'FAILFAST')).order.items
        ) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items struct 1 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | whole_document_output | shredded | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output shredded 1 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- MEASURE | whole_document_output | string | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output string 1 */
SELECT count(*) AS n, sum(octet_length(payload)) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`;

-- MEASURE | whole_document_output | struct | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output struct 1 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`;

-- MEASURE | whole_document_output | variant | repetition 1
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output variant 1 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`;

-- MEASURE | narrow | struct | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow struct 2 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | variant | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow variant 2 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | shredded | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow shredded 2 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | string | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow string 2 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | variant | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered variant 2 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | shredded | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered shredded 2 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | string | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered string 2 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | struct | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered struct 2 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | items | shredded | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items shredded 2 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | string | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items string 2 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2` t, LATERAL explode(
            from_json(t.payload, 'STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>', map('mode', 'FAILFAST')).order.items
        ) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | struct | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items struct 2 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | variant | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items variant 2 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | whole_document_output | string | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output string 2 */
SELECT count(*) AS n, sum(octet_length(payload)) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`;

-- MEASURE | whole_document_output | struct | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output struct 2 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`;

-- MEASURE | whole_document_output | variant | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output variant 2 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`;

-- MEASURE | whole_document_output | shredded | repetition 2
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output shredded 2 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- MEASURE | narrow | variant | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow variant 3 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | shredded | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow shredded 3 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | string | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow string 3 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | struct | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow struct 3 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | shredded | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered shredded 3 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | string | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered string 3 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | struct | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered struct 3 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | variant | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered variant 3 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | items | string | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items string 3 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2` t, LATERAL explode(
            from_json(t.payload, 'STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>', map('mode', 'FAILFAST')).order.items
        ) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | struct | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items struct 3 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | variant | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items variant 3 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | shredded | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items shredded 3 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | whole_document_output | struct | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output struct 3 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`;

-- MEASURE | whole_document_output | variant | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output variant 3 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`;

-- MEASURE | whole_document_output | shredded | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output shredded 3 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- MEASURE | whole_document_output | string | repetition 3
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output string 3 */
SELECT count(*) AS n, sum(octet_length(payload)) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`;

-- MEASURE | narrow | shredded | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow shredded 4 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | string | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow string 4 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | struct | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow struct 4 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | variant | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow variant 4 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | string | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered string 4 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | struct | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered struct 4 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | variant | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered variant 4 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | shredded | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered shredded 4 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | items | struct | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items struct 4 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | variant | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items variant 4 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | shredded | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items shredded 4 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | string | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items string 4 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2` t, LATERAL explode(
            from_json(t.payload, 'STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>', map('mode', 'FAILFAST')).order.items
        ) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | whole_document_output | variant | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output variant 4 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`;

-- MEASURE | whole_document_output | shredded | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output shredded 4 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- MEASURE | whole_document_output | string | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output string 4 */
SELECT count(*) AS n, sum(octet_length(payload)) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`;

-- MEASURE | whole_document_output | struct | repetition 4
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output struct 4 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`;

-- MEASURE | narrow | string | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow string 5 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | struct | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow struct 5 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | variant | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow variant 5 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | narrow | shredded | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure narrow shredded 5 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total, avg(amount) AS average FROM p GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | struct | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered struct 5 */
WITH p AS (SELECT payload.order.customer.tier AS tier, payload.order.currency AS currency,
            payload.order.total.base_total AS amount FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | variant | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered variant 5 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | shredded | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered shredded 5 */
WITH p AS (SELECT
            try_variant_get(payload, '$.order.customer.tier', 'string') AS tier,
            try_variant_get(payload, '$.order.currency', 'string') AS currency,
            try_variant_get(payload, '$.order.total.base_total', 'decimal(18,2)') AS amount
            FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | filtered | string | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure filtered string 5 */
WITH p AS (SELECT p.order.customer.tier AS tier, p.order.currency AS currency,
            p.order.total.base_total AS amount
            FROM (SELECT from_json(payload, 'STRUCT<order:STRUCT<currency:STRING,customer:STRUCT<tier:STRING>,total:STRUCT<base_total:DECIMAL(18,2)>>>', map('mode', 'FAILFAST')) AS p FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`)) SELECT tier, currency, count(*) AS n,
            sum(amount) AS total FROM p WHERE tier = 'gold' AND amount BETWEEN 100 AND 200
            GROUP BY tier, currency ORDER BY tier, currency;

-- MEASURE | items | variant | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items variant 5 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | shredded | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items shredded 5 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

-- MEASURE | items | string | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items string 5 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2` t, LATERAL explode(
            from_json(t.payload, 'STRUCT<order:STRUCT<items:ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>>>', map('mode', 'FAILFAST')).order.items
        ) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | items | struct | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure items struct 5 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

-- MEASURE | whole_document_output | shredded | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output shredded 5 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2`;

-- MEASURE | whole_document_output | string | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output string 5 */
SELECT count(*) AS n, sum(octet_length(payload)) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_string_2`;

-- MEASURE | whole_document_output | struct | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output struct 5 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2`;

-- MEASURE | whole_document_output | variant | repetition 5
SET use_cached_result = false;
/* vb_wh_1m_d10457b0 measure whole_document_output variant 5 */
SELECT count(*) AS n, sum(octet_length(to_json(payload))) AS output_bytes FROM `workspace`.`default`.`vb_20261023_193651_c739a157_variant_2`;
