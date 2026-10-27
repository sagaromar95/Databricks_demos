# Databricks demos

Small, self-contained demos of individual Databricks features. Each one is a
notebook (and sometimes companion SQL) that creates its own scratch tables,
validates correctness, and cleans up nothing automatically — so you can inspect
the results afterward.

These are teaching/benchmark artifacts, not production code. Elapsed times are
client-side and provisional; verify against query profiles before quoting numbers.

## Demos

| Demo | What it shows | Folder |
| --- | --- | --- |
| VARIANT shredding | Storage, write, and read comparison of JSON STRING vs STRUCT vs unshredded/shredded VARIANT; plus physical-layout inspection in Parquet | [`variant-shredding/`](variant-shredding/) |

_More to come (one folder per feature)._
