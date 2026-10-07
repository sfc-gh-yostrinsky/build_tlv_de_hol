# Data Engineering HOL: Glue lake + DCM (DEV/PROD) + dbt Fusion + Spark Code Bundle

## Overview

| Technology | Purpose | Output |
|---|---|---|
| **AWS Glue + Catalog-Linked Database** | Raw lake in Glue, read/written by Snowflake with Lake Formation vended credentials | `TLV_HOL_LAKE.tlv_hol_raw.{customers, products, orders}` (Iceberg v3) |
| **DCM Projects** | Everything downstream as code, deployed to DEV and PROD from one source tree | DBs, warehouses, DT, dbt project, code bundle, task graph |
| **Dynamic Table DAG** | 3 chained DTs, `REFRESH_MODE = ADAPTIVE`, `INITIALIZE = ON_SCHEDULE`, refreshed from the leaf by a serverless task | `DT_ORDER_LINES` -> `DT_HOURLY_SALES_METRICS` -> `DT_DAILY_CATEGORY_REGION` |
| **dbt Fusion 2.0.0** | dbt project running inside Snowflake | `CUSTOMER_LIFETIME_VALUE` (Iceberg v3, Snowflake-managed storage) |
| **Code Bundle (`type: spark`)** | PySpark on Snowpark Connect, with a PyPI dependency (`tabulate`) | `PRODUCT_CATEGORY_ANALYSIS` (Iceberg v3, Snowflake-managed storage) |
| **Task Graph** | Orchestrates DT DAG, dbt and Spark in parallel; every task publishes a return value | `TLV_PIPELINE` (STARTED, hourly) |

## Architecture

```
 AWS (S3 + Glue tlv_hol_raw + Lake Formation)
        │  Glue Iceberg REST, vended credentials
        ▼
 CATALOG INTEGRATION TLV_HOL_GLUE_CI ──► CLD TLV_HOL_LAKE          (shared by DEV + PROD)
        │
        ▼                        DCM project per environment (dcm/)
 ┌───────────────────────────── TLV_BUILD_HOL_DEV  /  TLV_BUILD_HOL ─────────────────────────────┐
 │  TLV_PIPELINE (task graph)                                                                     │
 │   T_START ─┬─ T_REFRESH_DT ─ DT_ORDER_LINES > DT_HOURLY_SALES_METRICS > DT_DAILY_CATEGORY_REGION │
 │            ├─ T_RUN_DBT    ─ CUSTOMER_LIFETIME_VALUE            (dbt 2.0.0, Iceberg v3)       ├─ T_FINISH
 │            └─ T_RUN_SPARK  ─ PRODUCT_CATEGORY_ANALYSIS          (type:spark, Iceberg v3)      │
 └────────────────────────────────────────────────────────────────────────────────────────────────┘
```

| | DEV | PROD |
|---|---|---|
| DCM project | `DCM_PROJECTS.PROJECTS.TLV_BUILD_HOL_DEV` | `DCM_PROJECTS.PROJECTS.TLV_BUILD_HOL_PROD` |
| Database | `TLV_BUILD_HOL_DEV` | `TLV_BUILD_HOL` |
| Warehouses | `TLV_{DT,DBT,SPARK}_WH_DEV` (XSMALL) | `TLV_{DT,DBT,SPARK}_WH` (SMALL) |
| dbt target | `dev` | `prod` |

## Layout

```
aws/setup_glue_lakeformation.sh   S3 bucket, Glue DB, IAM roles, Lake Formation grants
aws/teardown.sh                   remove the AWS side
sql/01_glue_catalog_cld.sql       catalog integration, CLD, raw Iceberg v3 tables + seed data
sql/99_cleanup.sql                Snowflake cleanup (after `snow dcm purge`)
dcm/manifest.yml                  DEV/PROD targets + templating
dcm/sources/definitions/*.sql     infrastructure, tables, DT DAG, dbt project
dcm/sources/definitions/pipeline.yml  code bundle + task graph (Pipeline Builder; schedule set here)
dcm/sources/dbt_ecommerce/        dbt project (Fusion 2.0.0; must be under sources/ for CLI deploys)
dcm/code_bundles/product_analysis/ type:spark bundle: code_bundle.yml, pyproject.toml, .py
```

## Run order

Prerequisites: AWS CLI logged in to the target account, Snowflake CLI 3.17+, role ACCOUNTADMIN,
and the parent schema `DCM_PROJECTS.PROJECTS`.

```bash
# 1. AWS side
./aws/setup_glue_lakeformation.sh

# 2. Shared raw lake. After the catalog integration is created, copy API_AWS_IAM_USER_ARN and
#    API_AWS_EXTERNAL_ID from DESC CATALOG INTEGRATION into the trust policy, then re-run the file:
snow sql -f sql/01_glue_catalog_cld.sql
./aws/setup_glue_lakeformation.sh trust <API_AWS_IAM_USER_ARN> <API_AWS_EXTERNAL_ID>
snow sql -f sql/01_glue_catalog_cld.sql

# 3. dbt version: DEFINE DBT PROJECT does not accept DBT_VERSION, so set the account default
snow sql -q "ALTER ACCOUNT SET DEFAULT_DBT_VERSION = '2.0.0'"

# 4. Register both DCM projects once
snow sql -q "CREATE DCM PROJECT IF NOT EXISTS DCM_PROJECTS.PROJECTS.TLV_BUILD_HOL_DEV"
snow sql -q "CREATE DCM PROJECT IF NOT EXISTS DCM_PROJECTS.PROJECTS.TLV_BUILD_HOL_PROD"

# 5. Plan and deploy each environment
cd dcm
snow dcm plan   --target DEV --save-output
snow dcm deploy --target DEV --alias <change>
#   ...same with --target PROD

# 6. Graphs deploy STARTED (hourly). Run one now:
snow sql -q "EXECUTE TASK GRAPH TLV_BUILD_HOL_DEV.DATA_ENG_DEMO.TLV_PIPELINE"
```

Monitor in Snowsight under **Transformation > Tasks**, or:

```sql
SELECT name, state, return_value, error_message
FROM TABLE(TLV_BUILD_HOL_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
WHERE database_name = 'TLV_BUILD_HOL_DEV'
ORDER BY scheduled_time DESC;

SELECT * FROM TLV_BUILD_HOL_DEV.DATA_ENG_DEMO.PIPELINE_RUN_LOG ORDER BY created_at DESC;
```

## Gotchas worth knowing

- **Glue identifiers are lowercase.** The CLD uses `CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE`, so SQL
  and dbt can write `TLV_HOL_LAKE.tlv_hol_raw.orders` unquoted. Snowpark Connect upper-cases names by
  default, so the Spark job sets `spark.sql.caseSensitive=true`.
- **Glue IRC needs `BASE_LOCATION`** on `CREATE ICEBERG TABLE` and does not support CTAS, so the seed
  tables are created first and then INSERTed.
- **dbt version.** `DEFINE DBT PROJECT` rejects `DBT_VERSION`; the version comes from the account
  parameter `DEFAULT_DBT_VERSION`.
- **DCM task clause order.** On child tasks, `COMMENT` must come before `AFTER`.
- **Task graphs.** Members of a task graph can't be run individually; use `EXECUTE TASK GRAPH`.
- **Spark output table.** The bundle recreates it every run with
  `writeTo(...).using("iceberg").tableProperty("format-version", "3").createOrReplace()`, with
  `snowpark.connect.iceberg.external_volume = SNOWFLAKE_MANAGED`.
- **DT DAG.** Every DT is `TARGET_LAG = DOWNSTREAM` and nothing consumes the leaf, so the DAG refreshes only
  when the serverless task `T_REFRESH_DT` refreshes the leaf (upstream DTs refresh first). `INITIALIZE` is
  immutable in DCM: switching an existing DT to `ON_SCHEDULE` means dropping it first.
- **Return values.** A Spark bundle can't call `SYSTEM$SET_RETURN_VALUE`, so `T_RUN_SPARK` sets it in SQL
  after the bundle returns. Read them in `TASK_HISTORY.RETURN_VALUE`.
- **Telemetry.** Each environment database sets `LOG_LEVEL`/`LOG_EVENT_LEVEL = TRACE`, `TRACE_LEVEL = ALWAYS`,
  `METRIC_LEVEL = ALL`; everything lands in `SNOWFLAKE.TELEMETRY.EVENTS`.

## Cleanup

```bash
cd dcm && snow dcm purge --target PROD && snow dcm purge --target DEV && cd ..
snow sql -f sql/99_cleanup.sql
./aws/teardown.sh
```

## Next: talk to your data

See **[intelligence/README.md](intelligence/README.md)** to build a semantic view and a Snowflake
Intelligence agent over `CUSTOMER_LIFETIME_VALUE` and `PRODUCT_CATEGORY_ANALYSIS`.
