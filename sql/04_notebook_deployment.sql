-- ============================================================================
-- DATA ENGINEERING DEMO - PART 4: NOTEBOOK PROJECT DEPLOYMENT
-- ============================================================================
-- This script shows how to deploy and execute notebooks in Snowflake
-- Uses: CREATE NOTEBOOK PROJECT -> EXECUTE NOTEBOOK PROJECT
-- 
-- Notebooks can be deployed from:
-- 1. A Snowsight Workspace (recommended for development)
-- 2. A Stage (for CI/CD pipelines)
-- ============================================================================

-- ============================================================================
-- WHAT ARE NOTEBOOKS IN WORKSPACES?
-- ============================================================================
/*
Snowflake Notebooks in Workspaces provide a full Jupyter-like experience 
directly in Snowsight, powered by Snowpark Container Services.

KEY BENEFITS:
  • Native Jupyter experience in Snowsight - no external infrastructure
  • Git integration via Workspaces for version control
  • Snowpark Python pre-installed - write DataFrames that execute in Snowflake
  • Access to Snowflake data without credentials or connection setup
  • Schedule as NOTEBOOK PROJECTS for automated execution

TWO MODES OF EXECUTION:
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  INTERACTIVE (development)        │  HEADLESS (production)             │
  │  ─────────────────────────        │  ────────────────────              │
  │  • Open notebook in Workspace     │  • CREATE NOTEBOOK PROJECT         │
  │  • Click cells, see output        │  • EXECUTE NOTEBOOK PROJECT        │
  │  • Great for exploration          │  • CI/CD friendly                  │
  └─────────────────────────────────────────────────────────────────────────┘

COMPUTE RESOURCES:
  • Compute Pool: Runs the Python kernel (Snowpark Container Services)
  • Query Warehouse: Executes SQL and Snowpark pushdown operations

SNOWPARK vs SNOWPARK CONNECT:
  • Snowpark: Native Snowflake Python API - works out of the box
  • Snowpark Connect: PySpark API for migrating Spark workloads (needs EAI)
*/
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TLV_BUILD_HOL;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO;
USE SCHEMA DATA_ENG_DEMO;

-- Create dedicated warehouse for notebook queries
CREATE WAREHOUSE IF NOT EXISTS TLV_NOTEBOOK_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- ============================================================================
-- OPTION A: DEPLOY FROM WORKSPACE (Recommended)
-- ============================================================================
-- Workspaces provide version control and collaborative development
-- The workspace path format: snow://workspace/USER$.SCHEMA."workspace_name"/versions/live

-- Then create the Notebook Project from that workspace:
CREATE OR REPLACE NOTEBOOK PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.product_analysis_project
    FROM 'snow://workspace/user$.public.HOL/versions/live/notebooks'
    COMMENT = 'Product category analysis using Snowpark Connect';

-- ============================================================================
-- OPTION B: DEPLOY FROM STAGE (CI/CD Pipelines)
-- ============================================================================
-- Stages are better for automated deployments from Git/CI systems

-- Create a stage for notebook files
-- CREATE STAGE IF NOT EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage
--     DIRECTORY = (ENABLE = TRUE);

-- Upload files to stage (run from terminal or use PUT):
/*
-- Terminal commands:
snow stage put notebooks/product_category_analysis_snowpark.ipynb @TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage --overwrite
snow stage put notebooks/requirements.txt @TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage --overwrite

-- Or SQL PUT (from Snowsight):
PUT file:///path/to/product_category_analysis_snowpark.ipynb @TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/requirements.txt @TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
*/

-- Verify files are uploaded
-- LIST @TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage;

-- Create Notebook Project from stage
-- CREATE OR REPLACE NOTEBOOK PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.product_analysis_project
--     FROM '@TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage'
--     COMMENT = 'Product category analysis using Snowpark Connect (SCOS)';

-- ============================================================================
-- EXECUTE NOTEBOOK PROJECT
-- ============================================================================
-- Runs the notebook in headless mode (non-interactive)
-- Requires: Compute Pool for Container Runtime

EXECUTE NOTEBOOK PROJECT TLV_BUILD_HOL.DATA_ENG_DEMO.product_analysis_project
    MAIN_FILE = 'product_category_analysis_snowpark.ipynb'
    COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
    QUERY_WAREHOUSE = 'TLV_NOTEBOOK_WH'
    RUNTIME = 'V2.2-CPU-PY3.11'
    ;

-- ============================================================================
-- VERIFY DEPLOYMENT
-- ============================================================================

-- List notebook projects
SHOW NOTEBOOK PROJECTS IN SCHEMA TLV_BUILD_HOL.DATA_ENG_DEMO;

-- Check output table (created by the notebook)
SELECT * FROM TLV_BUILD_HOL.DATA_ENG_DEMO.PRODUCT_CATEGORY_ANALYSIS 
ORDER BY overall_revenue_rank 
LIMIT 10;

-- ============================================================================
-- KEY CONCEPTS: NOTEBOOK PROJECT vs NOTEBOOK
-- ============================================================================
/*
┌─────────────────────────────────────────────────────────────────────────────┐
│                    NOTEBOOK vs NOTEBOOK PROJECT                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  NOTEBOOK (CREATE NOTEBOOK)                                                 │
│  ──────────────────────────                                                 │
│  • Interactive execution in Snowsight                                       │
│  • Great for development and exploration                                    │
│  • Cannot be scheduled or called from Tasks                                 │
│                                                                             │
│  ───────────────────────────────────────────────────────────────────────── │
│                                                                             │
│  NOTEBOOK PROJECT (CREATE NOTEBOOK PROJECT)                                 │
│  ──────────────────────────────────────────                                 │
│  • Headless/non-interactive execution                                       │
│  • Can be scheduled with Tasks                                              │
│  • Supports CI/CD pipelines                                                 │
│  • Use REQUIREMENTS_FILE for reproducible environments                      │
│  • Deploy from Workspace OR Stage                                           │
│                                                                             │
│  USE CASE FLOW:                                                             │
│  1. Develop interactively in Workspace (NOTEBOOK)                           │
│  2. Test and iterate                                                        │
│  3. Deploy as NOTEBOOK PROJECT for production                               │
│  4. Schedule with Tasks for automation                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- CLEANUP
-- ============================================================================
/*
DROP NOTEBOOK PROJECT IF EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO.product_analysis_project;
DROP STAGE IF EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO.notebook_stage;
*/
