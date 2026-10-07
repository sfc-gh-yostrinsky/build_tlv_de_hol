-- Database, schema and compute for one environment of the TLV DE HOL.
-- {{env_suffix}} is '_DEV' for DEV and '' for PROD (see manifest.yml).

-- Most verbose telemetry for everything in the database (tasks, DTs, dbt, code
-- bundles), sent to the account event table (SNOWFLAKE.TELEMETRY.EVENTS).
DEFINE DATABASE TLV_BUILD_HOL{{env_suffix}}
    LOG_LEVEL = TRACE
    LOG_EVENT_LEVEL = TRACE
    TRACE_LEVEL = ALWAYS
    METRIC_LEVEL = ALL
    COMMENT = 'TLV DE HOL transformation layer ({{dbt_target}}). Managed by DCM. Drop after HOL. Reviewed 2026-10-05.';

DEFINE SCHEMA TLV_BUILD_HOL{{env_suffix}}.DATA_ENG_DEMO
    COMMENT = 'TLV DE HOL DT, dbt models, Spark output and pipeline tasks ({{dbt_target}}). Managed by DCM.';

DEFINE WAREHOUSE TLV_DT_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'TLV DE HOL dynamic table refreshes ({{dbt_target}}). Managed by DCM. Drop after HOL.';

DEFINE WAREHOUSE TLV_DBT_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'TLV DE HOL dbt project runs ({{dbt_target}}). Managed by DCM. Drop after HOL.';

DEFINE WAREHOUSE TLV_SPARK_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'TLV DE HOL type:spark code bundle jobs and pipeline tasks ({{dbt_target}}). Managed by DCM. Drop after HOL.';
