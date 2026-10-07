-- ============================================================================
-- TLV DE HOL - CLEANUP (Snowflake side). Then run ./aws/teardown.sh.
-- ============================================================================
-- Order matters:
--   1. Purge each DCM project: drops every object it manages (DBs, WHs, DT, dbt
--      project, code bundle, tasks). A project that manages a code bundle can't
--      be dropped until it has been purged.
--        cd dcm
--        snow dcm purge --target PROD
--        snow dcm purge --target DEV
--   2. Then run this script.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Created by the Spark bundle, not DCM. Purge drops the databases, so these
-- normally go with them; listed for clarity.
DROP ICEBERG TABLE IF EXISTS TLV_BUILD_HOL_DEV.DATA_ENG_DEMO.PRODUCT_CATEGORY_ANALYSIS;
DROP ICEBERG TABLE IF EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO.PRODUCT_CATEGORY_ANALYSIS;

DROP DCM PROJECT IF EXISTS DCM_PROJECTS.PROJECTS.TLV_BUILD_HOL_DEV;
DROP DCM PROJECT IF EXISTS DCM_PROJECTS.PROJECTS.TLV_BUILD_HOL_PROD;

-- Shared raw lake. aws/teardown.sh removes the Glue tables and S3 data either way
-- (whether dropping a write-enabled CLD also drops its Glue tables is not verified
-- here). Integration last (identity).
DROP DATABASE IF EXISTS TLV_HOL_LAKE;
DROP CATALOG INTEGRATION IF EXISTS TLV_HOL_GLUE_CI;

-- Account default set for this HOL (was 1.9.4 before).
-- ALTER ACCOUNT UNSET DEFAULT_DBT_VERSION;

SHOW DATABASES LIKE 'TLV%';
SHOW WAREHOUSES LIKE 'TLV%';
SHOW CATALOG INTEGRATIONS LIKE 'TLV%';
