# VARIANT shredding

Demos for Databricks' open VARIANT type and its physical shredding optimization:
storage/write/read comparison against JSON STRING and STRUCT, plus inspection of
the shredded Parquet layout.

## Feature status — recorded as of 2026-10-27

| Feature | Status | Minimum compute |
| --- | --- | --- |
| VARIANT data type | GA (AWS · Azure · GCP) | Introduced DBR 15.3 (Public Preview) |
| VARIANT shredding (`delta.enableVariantShredding`) | Available since DBR 17.3 LTS; docs carry no preview notice (treat as GA) | DBR 17.3 LTS (18.1+ recommended for variant statistics / data skipping) |

Status is the same across AWS, Azure, and GCP for both features. Status and
per-cloud availability can change — re-verify against the release notes before a
production decision.

## Docs & references

| Resource | Source | Link |
| --- | --- | --- |
| VARIANT shredding | Azure Databricks docs | https://learn.microsoft.com/azure/databricks/delta/variant-shredding |
| Query variant data (VARIANT type) | Azure Databricks docs | https://learn.microsoft.com/azure/databricks/semi-structured/variant |
| VARIANT type — release note (introduced, Public Preview) | DBR 15.3 release notes | https://learn.microsoft.com/azure/databricks/archive/runtime-release-notes/15.3 |
| VARIANT shredding — release note (runtime requirement) | DBR 17.3 LTS release notes | https://learn.microsoft.com/azure/databricks/release-notes/runtime/17.3lts |
| VARIANT now generally available | Databricks blog | https://www.databricks.com/blog/ingest-semi-structured-data-faster-and-more-efficiently-variant-now-generally-available |
| Variant Shredding — spec | Apache Parquet | https://parquet.apache.org/docs/file-format/types/variantshredding/ |
| `parquet-format` (VariantShredding.md) | Apache Parquet repo | https://github.com/apache/parquet-format/blob/master/VariantShredding.md |
| `common/variant` README | Apache Spark repo | https://github.com/apache/spark/blob/master/common/variant/README.md |

## Contents

See each notebook / SQL file in this folder for its own run instructions and
requirements.
