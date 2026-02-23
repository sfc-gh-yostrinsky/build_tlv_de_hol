-- ============================================================================
-- DATA ENGINEERING DEMO - PART 5: TASKS DAG ORCHESTRATION
-- ============================================================================
-- Creates a Task Graph (DAG) that orchestrates all pipeline components:
-- - Dynamic Table refresh
-- - dbt project execution (EXECUTE DBT PROJECT)
-- - Notebook project execution (EXECUTE NOTEBOOK PROJECT)
-- All pipelines run in PARALLEL after root task triggers
--
-- NOTE: Run this script in Snowsight (Snowflake CLI splits on semicolons)
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TLV_BUILD_HOL;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO;
USE SCHEMA DATA_ENG_DEMO;

-- ============================================================================
-- SUSPEND EXISTING TASKS (if they exist)
-- ============================================================================
ALTER TASK IF EXISTS task_pipeline_root SUSPEND;
ALTER TASK IF EXISTS task_pipeline_finalizer SUSPEND;
ALTER TASK IF EXISTS task_refresh_dynamic_table SUSPEND;
ALTER TASK IF EXISTS task_run_dbt_model SUSPEND;
ALTER TASK IF EXISTS task_run_notebook SUSPEND;

-- ============================================================================
-- PREREQUISITE: DEPLOY DBT PROJECT (run in terminal first)
-- ============================================================================
/*
Before creating the task, deploy the dbt project using snow CLI:

snow dbt deploy dbt_ecommerce \
  --source ./dbt_ecommerce \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO

Verify deployment:
snow dbt list --in schema DATA_ENG_DEMO --database TLV_BUILD_HOL
*/

-- ============================================================================
-- PREREQUISITE: CREATE NOTEBOOK PROJECT
-- ============================================================================
-- Run sql/04_notebook_deployment.sql first to create the notebook project
-- The notebook project can be created from:
--   - A Workspace (recommended): snow://workspace/USER$.SCHEMA."workspace"/versions/live
--   - A Stage (for CI/CD): @database.schema.stage_name

-- ============================================================================
-- LOGGING TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id VARCHAR,
    pipeline_name VARCHAR,
    status VARCHAR,
    started_at TIMESTAMP_LTZ,
    completed_at TIMESTAMP_LTZ,
    summary VARCHAR,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- TASK GRAPH DEFINITION
-- ============================================================================

-- ROOT TASK: Scheduler (runs every hour or can be triggered manually)
CREATE OR ALTER TASK task_pipeline_root
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    USER_TASK_TIMEOUT_MS = 300000  -- 5 minutes
    CONFIG = '{"pipeline_name": "e-commerce_analytics", "environment": "demo"}'
AS
DECLARE
    cnt INTEGER;
    msg STRING;
BEGIN
    INSERT INTO pipeline_run_log (run_id, pipeline_name, status, started_at)
    SELECT UUID_STRING(), 'e-commerce_analytics', 'STARTED', CURRENT_TIMESTAMP();
    cnt := (SELECT COUNT(*) FROM pipeline_run_log);
    msg := 'Pipeline started. Log records: ' || cnt::STRING;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

-- ============================================================================
-- CHILD TASK 1: Refresh Dynamic Table (runs in parallel)
-- ============================================================================
CREATE OR ALTER TASK task_refresh_dynamic_table
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    USER_TASK_TIMEOUT_MS = 120000  -- 2 minutes
    AFTER task_pipeline_root
AS
DECLARE
    cnt INTEGER;
    msg STRING;
BEGIN
    ALTER DYNAMIC TABLE dt_hourly_sales_metrics REFRESH;
    cnt := (SELECT COUNT(*) FROM dt_hourly_sales_metrics);
    msg := 'DT refreshed. Records: ' || cnt::STRING;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

-- ============================================================================
-- CHILD TASK 2: Execute dbt Project (runs in parallel)
-- ============================================================================
-- Uses EXECUTE DBT PROJECT - the native way to run dbt in Snowflake
CREATE OR ALTER TASK task_run_dbt_model
    WAREHOUSE = TLV_DBT_WH
    USER_TASK_TIMEOUT_MS = 300000  -- 5 minutes
    AFTER task_pipeline_root
AS
DECLARE
    cnt INTEGER;
    msg STRING;
BEGIN
    EXECUTE DBT PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.DBT_ECOMMERCE 
        ARGS = 'run --select customer_lifetime_value';
    cnt := (SELECT COUNT(*) FROM customer_lifetime_value);
    msg := 'dbt completed. CLV records: ' || cnt::STRING;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

-- ============================================================================
-- CHILD TASK 3: Execute Notebook Project (runs in parallel)
-- ============================================================================
-- Uses EXECUTE NOTEBOOK PROJECT - runs notebook in headless mode
-- Note: EXECUTE PROJECT statements cannot be wrapped in scripting blocks
CREATE OR ALTER TASK task_run_notebook
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    USER_TASK_TIMEOUT_MS = 600000  -- 10 minutes
    AFTER task_pipeline_root
AS
EXECUTE NOTEBOOK PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.product_analysis_project
    MAIN_FILE = 'product_category_analysis_snowpark.ipynb'
    COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
    QUERY_WAREHOUSE = 'TLV_NOTEBOOK_WH'
    RUNTIME = 'V2.2-CPU-PY3.11'
    ;

-- ============================================================================
-- FINALIZER TASK: Runs after all child tasks complete
-- ============================================================================
CREATE OR ALTER TASK task_pipeline_finalizer
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    USER_TASK_TIMEOUT_MS = 60000  -- 1 minute
    FINALIZE = task_pipeline_root
AS
DECLARE
    cnt INTEGER;
    msg STRING;
BEGIN
    INSERT INTO pipeline_run_log (run_id, pipeline_name, status, completed_at)
    SELECT UUID_STRING(), 'e-commerce_analytics', 'COMPLETED', CURRENT_TIMESTAMP();
    cnt := (SELECT COUNT(*) FROM pipeline_run_log);
    msg := 'Pipeline completed. Log records: ' || cnt::STRING;
    CALL SYSTEM$SET_RETURN_VALUE(:msg);
END;

-- ============================================================================
-- ENABLE THE TASK GRAPH
-- ============================================================================

-- Resume all tasks in the DAG (child tasks first, then root)
ALTER TASK task_pipeline_finalizer RESUME;
ALTER TASK task_refresh_dynamic_table RESUME;
ALTER TASK task_run_dbt_model RESUME;
ALTER TASK task_run_notebook RESUME;

-- Finally, resume the root task to start the schedule
ALTER TASK task_pipeline_root RESUME;

-- Or use helper to resume all at once:
-- SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('task_pipeline_root');

-- ============================================================================
-- MANUAL EXECUTION (for demo purposes)
-- ============================================================================
EXECUTE TASK task_pipeline_root;

-- ============================================================================
-- MONITORING QUERIES
-- ============================================================================

-- View all tasks in the DAG
SHOW TASKS LIKE 'TASK_%' IN SCHEMA TLV_BUILD_HOL.DATA_ENG_DEMO;

-- View task dependencies
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'TLV_BUILD_HOL.DATA_ENG_DEMO.TASK_PIPELINE_ROOT',
    RECURSIVE => TRUE
));

-- View recent task execution history
SELECT 
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    DATEDIFF('second', query_start_time, completed_time) AS duration_seconds,
    return_value,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    TASK_NAME => 'TASK_PIPELINE_ROOT'
))
ORDER BY scheduled_time DESC
LIMIT 20;

-- Check pipeline run log
SELECT * FROM pipeline_run_log ORDER BY created_at DESC LIMIT 10;
