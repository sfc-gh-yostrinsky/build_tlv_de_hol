-- ============================================================================
-- DATA ENGINEERING DEMO - CLEANUP SCRIPT
-- ============================================================================
-- Removes all demo objects created by scripts 01-05
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- DROP DATABASE (removes all schemas, tables, tasks, projects, stages inside)
-- ============================================================================
DROP DATABASE IF EXISTS TLV_BUILD_HOL;

-- ============================================================================
-- DROP ACCOUNT-LEVEL OBJECTS
-- ============================================================================
-- These exist outside the database and must be dropped separately

-- External Access Integration (created in script 04)
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TLV_BUILD_HOL_PYPI_EAI;

-- Catalog Integration (created in script 01)
DROP CATALOG INTEGRATION IF EXISTS tlv_iceberg_catalog_int;
DROP CATALOG INTEGRATION IF EXISTS iceberg_files_catalog_int;

-- External Volume (created in script 01)
DROP EXTERNAL VOLUME IF EXISTS tlv_datalake_s3_ev;
DROP EXTERNAL VOLUME IF EXISTS build_tlv_2026_ev;

-- Warehouses (created in scripts 02-04)
DROP WAREHOUSE IF EXISTS TLV_DT_WH;
DROP WAREHOUSE IF EXISTS TLV_DBT_WH;
DROP WAREHOUSE IF EXISTS TLV_NOTEBOOK_WH;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW EXTERNAL VOLUMES LIKE '%tlv%';
SHOW CATALOG INTEGRATIONS LIKE '%iceberg%';
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '%tlv%';
