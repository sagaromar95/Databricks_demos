-- Run all statements sequentially in one SQL editor session on the same warehouse.
-- Existing tables are read only. Each query should return identical category totals.
SET use_cached_result = false;

/* vb_items_20261028_171826 warmup struct 0 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 warmup shredded 0 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 warmup shredded_typed 0 */
SELECT
    e.item.product.category AS category,
    count(*) AS n,
    sum(e.item.quantity) AS quantity
FROM workspace.default.vb_20261023_193651_c739a157_shredded_2 AS t,
LATERAL explode(
    variant_get(
        t.payload,
        '$.order.items',
        'ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>'
    )
) AS e(item)
GROUP BY 1
ORDER BY 1;

/* vb_items_20261028_171826 measure struct 1 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded 1 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded_typed 1 */
SELECT
    e.item.product.category AS category,
    count(*) AS n,
    sum(e.item.quantity) AS quantity
FROM workspace.default.vb_20261023_193651_c739a157_shredded_2 AS t,
LATERAL explode(
    variant_get(
        t.payload,
        '$.order.items',
        'ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>'
    )
) AS e(item)
GROUP BY 1
ORDER BY 1;

/* vb_items_20261028_171826 measure shredded 2 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded_typed 2 */
SELECT
    e.item.product.category AS category,
    count(*) AS n,
    sum(e.item.quantity) AS quantity
FROM workspace.default.vb_20261023_193651_c739a157_shredded_2 AS t,
LATERAL explode(
    variant_get(
        t.payload,
        '$.order.items',
        'ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>'
    )
) AS e(item)
GROUP BY 1
ORDER BY 1;

/* vb_items_20261028_171826 measure struct 2 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded_typed 3 */
SELECT
    e.item.product.category AS category,
    count(*) AS n,
    sum(e.item.quantity) AS quantity
FROM workspace.default.vb_20261023_193651_c739a157_shredded_2 AS t,
LATERAL explode(
    variant_get(
        t.payload,
        '$.order.items',
        'ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>'
    )
) AS e(item)
GROUP BY 1
ORDER BY 1;

/* vb_items_20261028_171826 measure struct 3 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded 3 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure struct 4 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded 4 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded_typed 4 */
SELECT
    e.item.product.category AS category,
    count(*) AS n,
    sum(e.item.quantity) AS quantity
FROM workspace.default.vb_20261023_193651_c739a157_shredded_2 AS t,
LATERAL explode(
    variant_get(
        t.payload,
        '$.order.items',
        'ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>'
    )
) AS e(item)
GROUP BY 1
ORDER BY 1;

/* vb_items_20261028_171826 measure shredded 5 */
SELECT try_variant_get(e.value, '$.product.category', 'string') AS category, count(*) AS n, sum(try_variant_get(e.value, '$.quantity', 'bigint')) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_shredded_2` t, LATERAL variant_explode(t.payload:order.items) AS e GROUP BY 1 ORDER BY 1;

/* vb_items_20261028_171826 measure shredded_typed 5 */
SELECT
    e.item.product.category AS category,
    count(*) AS n,
    sum(e.item.quantity) AS quantity
FROM workspace.default.vb_20261023_193651_c739a157_shredded_2 AS t,
LATERAL explode(
    variant_get(
        t.payload,
        '$.order.items',
        'ARRAY<STRUCT<product:STRUCT<category:STRING>,quantity:BIGINT>>'
    )
) AS e(item)
GROUP BY 1
ORDER BY 1;

/* vb_items_20261028_171826 measure struct 5 */
SELECT e.item.product.category AS category, count(*) AS n, sum(e.item.quantity) AS quantity FROM `workspace`.`default`.`vb_20261023_193651_c739a157_struct_2` t, LATERAL explode(t.payload.order.items) AS e(item) GROUP BY 1 ORDER BY 1;
