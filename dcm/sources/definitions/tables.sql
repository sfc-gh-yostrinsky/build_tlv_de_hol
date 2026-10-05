-- Run log written by the pipeline's first and last tasks.

DEFINE TABLE TLV_BUILD_HOL{{env_suffix}}.DATA_ENG_DEMO.PIPELINE_RUN_LOG (
    RUN_ID        VARCHAR,
    PIPELINE_NAME VARCHAR,
    STATUS        VARCHAR,
    SUMMARY       VARCHAR,
    CREATED_AT    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'TLV DE HOL pipeline run log ({{dbt_target}}). Managed by DCM.';
