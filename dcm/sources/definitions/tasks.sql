-- Pipeline task graph:
--
--   T_START ──┬── T_REFRESH_DT   (serverless: refresh leaf DT -> whole DT DAG)
--             ├── T_RUN_DBT      (dbt build -> Iceberg v3 CLV)                ── T_FINISH
--             └── T_RUN_SPARK    (type:spark bundle -> recreates Iceberg v3 output)
--
-- Early-access DCM: DEFINE TASK GRAPH owns the schedule. STARTED, so it runs on
-- schedule after every deploy; run on demand with EXECUTE TASK GRAPH.
-- Every task publishes a return value (SYSTEM$SET_RETURN_VALUE), visible in
-- TASK_HISTORY.RETURN_VALUE and in Snowsight's task graph run view. A Spark bundle
-- can't set it itself, so T_RUN_SPARK sets it in SQL after the bundle returns.
-- SYSTEM$SET_RETURN_VALUE only accepts a constant or bound variable, so each
-- message is built into :msg first.

{% set S = 'TLV_BUILD_HOL' ~ env_suffix ~ '.DATA_ENG_DEMO' %}

DEFINE TASK {{S}}.T_START
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'TLV DE HOL pipeline root: log START ({{dbt_target}}). Serverless. Managed by DCM.'
AS
BEGIN
    INSERT INTO {{S}}.PIPELINE_RUN_LOG (RUN_ID, PIPELINE_NAME, STATUS)
    SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'), 'tlv_hol_pipeline', 'STARTED';
    LET msg STRING := 'started {{dbt_target}} run ' || SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

DEFINE TASK {{S}}.T_REFRESH_DT
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'TLV DE HOL: refresh leaf DT DT_DAILY_CATEGORY_REGION, which refreshes its upstream DTs ({{dbt_target}}). Serverless. Managed by DCM.'
    AFTER {{S}}.T_START
AS
BEGIN
    ALTER DYNAMIC TABLE {{S}}.DT_DAILY_CATEGORY_REGION REFRESH;
    LET lines INTEGER := (SELECT COUNT(*) FROM {{S}}.DT_ORDER_LINES);
    LET hourly INTEGER := (SELECT COUNT(*) FROM {{S}}.DT_HOURLY_SALES_METRICS);
    LET daily INTEGER := (SELECT COUNT(*) FROM {{S}}.DT_DAILY_CATEGORY_REGION);
    LET msg STRING := 'DT DAG refreshed: order_lines=' || :lines || ' hourly=' || :hourly || ' daily=' || :daily;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

DEFINE TASK {{S}}.T_RUN_DBT
    WAREHOUSE = TLV_DBT_WH{{env_suffix}}
    COMMENT = 'TLV DE HOL: dbt build (run + test) of the Iceberg v3 CLV model ({{dbt_target}}). Managed by DCM.'
    AFTER {{S}}.T_START
AS
BEGIN
    EXECUTE DBT PROJECT {{S}}.DBT_ECOMMERCE ARGS = 'build --target {{dbt_target}}';
    LET clv INTEGER := (SELECT COUNT(*) FROM {{S}}.CUSTOMER_LIFETIME_VALUE);
    LET platinum INTEGER := (SELECT COUNT(*) FROM {{S}}.CUSTOMER_LIFETIME_VALUE WHERE customer_tier = 'Platinum');
    LET msg STRING := 'dbt build ok: customer_lifetime_value=' || :clv || ' platinum=' || :platinum;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

DEFINE TASK {{S}}.T_RUN_SPARK
    WAREHOUSE = TLV_SPARK_WH{{env_suffix}}
    COMMENT = 'TLV DE HOL: type:spark product analysis bundle ({{dbt_target}}). Managed by DCM.'
    AFTER {{S}}.T_START
AS
BEGIN
    EXECUTE CODE BUNDLE {{S}}.CB_PRODUCT_ANALYSIS
        ENTRYPOINT = 'product_category_analysis.py'
        ARGUMENTS = ('--target-db', 'TLV_BUILD_HOL{{env_suffix}}', '--lake-db', '{{lake_db}}', '--lake-schema', '{{lake_schema}}');
    LET n INTEGER := (SELECT COUNT(*) FROM {{S}}.PRODUCT_CATEGORY_ANALYSIS);
    LET top_cat STRING := (SELECT category FROM {{S}}.PRODUCT_CATEGORY_ANALYSIS ORDER BY overall_revenue_rank LIMIT 1);
    LET msg STRING := 'spark bundle ok: product_category_analysis=' || :n || ' top_category=' || :top_cat;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

DEFINE TASK {{S}}.T_FINISH
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'TLV DE HOL pipeline end: log row counts ({{dbt_target}}). Serverless. Managed by DCM.'
    AFTER {{S}}.T_REFRESH_DT, {{S}}.T_RUN_DBT, {{S}}.T_RUN_SPARK
AS
BEGIN
    LET summary STRING := 'dt_daily=' || (SELECT COUNT(*) FROM {{S}}.DT_DAILY_CATEGORY_REGION)
        || ' clv=' || (SELECT COUNT(*) FROM {{S}}.CUSTOMER_LIFETIME_VALUE)
        || ' spark=' || (SELECT COUNT(*) FROM {{S}}.PRODUCT_CATEGORY_ANALYSIS);
    INSERT INTO {{S}}.PIPELINE_RUN_LOG (RUN_ID, PIPELINE_NAME, STATUS, SUMMARY)
    SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'), 'tlv_hol_pipeline', 'COMPLETED', :summary;
    LET msg STRING := 'completed: ' || :summary;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

DEFINE TASK GRAPH {{S}}.TLV_PIPELINE
    SCHEDULE = '{{pipeline_schedule}}'
    ROOTS = ({{S}}.T_START)
    COMMENT = 'TLV DE HOL pipeline: DT DAG + dbt + Spark ({{dbt_target}}). Managed by DCM.'
    TASK_DEFAULTS (
        USER_TASK_TIMEOUT_MS = 1800000
    )
    STARTED
;
