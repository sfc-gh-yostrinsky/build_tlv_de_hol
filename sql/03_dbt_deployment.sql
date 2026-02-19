-- ============================================================================
-- DATA ENGINEERING DEMO - PART 3: DBT PROJECT DEPLOYMENT & EXECUTION
-- ============================================================================
-- This script shows how to deploy and run dbt projects natively in Snowflake
-- 
-- TWO DEPLOYMENT OPTIONS:
--   OPTION A: From a Snowsight Workspace (SQL - can run directly)
--   OPTION B: Using Snowflake CLI (terminal commands)
--
-- NOTE: Task scheduling is in script 05_tasks_dag.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TLV_BUILD_HOL;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO;
USE SCHEMA DATA_ENG_DEMO;

-- Create dedicated warehouse for dbt projects
CREATE WAREHOUSE IF NOT EXISTS TLV_DBT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- ============================================================================
-- OPTION A: DEPLOY FROM WORKSPACE (SQL)
-- ============================================================================
-- Workspaces provide version control and can be deployed via SQL.
--
-- PREREQUISITE: Create a workspace in Snowsight first:
--   1. Go to Projects > Workspaces
--   2. Click "+ Workspace"
--   3. Name it "dbt_ecommerce_workspace"
--   4. Upload your dbt project files (dbt_project.yml, models/, etc.)
--
-- The workspace path format is:
--   snow://workspace/<USER>.<SCHEMA>."<workspace_name>"/versions/live

-- Create the dbt project from workspace
CREATE OR REPLACE DBT PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.DBT_ECOMMERCE
    FROM 'snow://workspace/user$.public.HOL/versions/live/dbt_ecommerce'
    COMMENT = 'Customer lifetime value model for e-commerce analytics';

-- Verify deployment
SHOW DBT PROJECTS IN SCHEMA TLV_BUILD_HOL.DATA_ENG_DEMO;

-- ============================================================================
-- OPTION B: DEPLOY USING CLI (Terminal)
-- ============================================================================
-- Alternative: Use Snowflake CLI for deployment (better for CI/CD)
/*
# Deploy from local directory
snow dbt deploy dbt_ecommerce \
  --source ./dbt_ecommerce \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO

# Verify deployment
snow dbt list --in schema DATA_ENG_DEMO --database TLV_BUILD_HOL

# Check versions
SHOW VERSIONS IN DBT PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.DBT_ECOMMERCE;
*/

-- ============================================================================
-- EXECUTE DBT PROJECT (SQL)
-- ============================================================================
-- Run the dbt models to materialize the customer_lifetime_value table

-- Run all models
EXECUTE DBT PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.DBT_ECOMMERCE
    ARGS = 'run';

-- Or run specific model
EXECUTE DBT PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.DBT_ECOMMERCE
    ARGS = 'run --select customer_lifetime_value';

-- ============================================================================
-- ALTERNATIVE: EXECUTE USING CLI (Terminal)
-- ============================================================================
/*
# Preview model output WITHOUT creating objects (like dbt show)
snow dbt execute -c default \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO \
  dbt_ecommerce show --select customer_lifetime_value

# Run models to create tables/views
snow dbt execute -c default \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO \
  dbt_ecommerce run

# Run specific model with upstream dependencies
snow dbt execute -c default \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO \
  dbt_ecommerce run --select +customer_lifetime_value

# Run tests
snow dbt execute -c default \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO \
  dbt_ecommerce test

# Full build (run + test)
snow dbt execute -c default \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO \
  dbt_ecommerce build
*/

-- ============================================================================
-- VERIFY RESULTS
-- ============================================================================

-- Check the materialized table
SELECT * FROM customer_lifetime_value LIMIT 10;

-- Customer tier distribution
SELECT 
    customer_tier,
    COUNT(*) as customer_count,
    ROUND(AVG(total_revenue), 2) as avg_revenue,
    ROUND(AVG(ltv_score), 2) as avg_ltv_score
FROM customer_lifetime_value
GROUP BY customer_tier
ORDER BY avg_ltv_score DESC;

-- Top customers by LTV
SELECT 
    customer_name,
    segment,
    region,
    total_orders,
    total_revenue,
    ltv_score,
    customer_tier
FROM customer_lifetime_value
WHERE ltv_score IS NOT NULL
ORDER BY ltv_score DESC
LIMIT 10;

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
/*
┌─────────────────────────────────────────────────────────────────────────────┐
│              SNOWFLAKE DBT vs STANDARD DBT CLI                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STANDARD DBT CLI (local)             SNOWFLAKE-NATIVE DBT                  │
│  ────────────────────────             ────────────────────                  │
│  • Runs on your machine               • Runs INSIDE Snowflake               │
│  • pip install dbt-snowflake          • Snowflake CLI (snow)                │
│  • dbt run, dbt test                  • EXECUTE DBT PROJECT (SQL)           │
│  • Creds in profiles.yml              • No creds needed                     │
│                                                                             │
│  DEPLOYMENT OPTIONS:                                                        │
│  ───────────────────                                                        │
│  • FROM workspace (SQL)  → CREATE DBT PROJECT ... FROM 'snow://workspace/.' │
│  • snow dbt deploy (CLI) → Deploy from local directory                      │
│                                                                             │
│  EXECUTION OPTIONS:                                                         │
│  ──────────────────                                                         │
│  • EXECUTE DBT PROJECT (SQL) → Run from Snowflake SQL                       │
│  • snow dbt execute (CLI)    → Run from terminal                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- CLEANUP
-- ============================================================================
/*
DROP TABLE IF EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO.CUSTOMER_LIFETIME_VALUE;
DROP DBT PROJECT IF EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO.DBT_ECOMMERCE;
*/
