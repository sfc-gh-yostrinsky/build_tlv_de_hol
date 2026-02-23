-- ============================================================================
-- CUSTOMER SCENARIO: EXTERNALLY-MANAGED ICEBERG TABLES
-- ============================================================================
-- 
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  📌 WHAT IS THIS SCRIPT?                                                  ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  This shows how a CUSTOMER brings EXISTING Iceberg tables into Snowflake.║
-- ║                                                                           ║
-- ║  REAL WORLD: Customer has Iceberg tables in S3 written by Spark/Flink.   ║
-- ║  DEMO: We use pre-created Iceberg tables in S3 for this hands-on lab.    ║
-- ║                                                                           ║
-- ║  KEY POINT: Snowflake reads the Iceberg metadata file to understand      ║
-- ║  the table schema, partitions, and data file locations.                  ║
-- ║  NO DATA IS COPIED - Snowflake queries the files directly!               ║
-- ║                                                                           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- STEP 1: CREATE DATABASE & SCHEMA
-- ============================================================================
-- This is the customer's Snowflake environment where they'll access their
-- existing data lake tables.

CREATE DATABASE IF NOT EXISTS TLV_BUILD_HOL;
CREATE SCHEMA IF NOT EXISTS TLV_BUILD_HOL.EXTERNAL_ICEBERG;
USE DATABASE TLV_BUILD_HOL;
USE SCHEMA EXTERNAL_ICEBERG;

-- ============================================================================
-- STEP 2: CREATE EXTERNAL VOLUME
-- ============================================================================
-- Points to the S3 bucket where the Iceberg data files live.
-- Customer would configure this to their existing data lake storage.

CREATE OR REPLACE EXTERNAL VOLUME tlv_datalake_s3_ev
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'customer_s3_storage'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://build-tlv-2026/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/build-tlv-s3-role'
            STORAGE_AWS_EXTERNAL_ID = 'BUILD_TLV_HOL'
         )
      )
    ALLOW_WRITES = false;
    
DESC EXTERNAL VOLUME tlv_datalake_s3_ev;

-- ============================================================================
-- STEP 3: CREATE CATALOG INTEGRATION
-- ============================================================================
-- For externally-managed Iceberg, we need CATALOG_SOURCE = OBJECT_STORE.
-- This tells Snowflake to read table metadata from files (not a catalog service).

CREATE OR REPLACE CATALOG INTEGRATION tlv_iceberg_catalog_int
    CATALOG_SOURCE = OBJECT_STORE
    TABLE_FORMAT = ICEBERG
    ENABLED = TRUE;

-- ============================================================================
-- STEP 4: CREATE EXTERNALLY-MANAGED ICEBERG TABLES
-- ============================================================================
--
-- HOW IT WORKS:
--   1. Snowflake reads the metadata.json file
--   2. Metadata contains: schema, partition spec, data file locations
--   3. Snowflake auto-detects columns - no need to define them!
--   4. Queries read directly from Parquet files in S3

-- CUSTOMERS TABLE
CREATE OR REPLACE ICEBERG TABLE ext_customers
    EXTERNAL_VOLUME = 'tlv_datalake_s3_ev'
    CATALOG = 'tlv_iceberg_catalog_int'
    METADATA_FILE_PATH = 'customers_ice.tug9cOq0/metadata/00001-55bf24ff-46ed-4644-b4be-164ac2d4463c.metadata.json';

-- PRODUCTS TABLE
CREATE OR REPLACE ICEBERG TABLE ext_products
    EXTERNAL_VOLUME = 'tlv_datalake_s3_ev'
    CATALOG = 'tlv_iceberg_catalog_int'
    METADATA_FILE_PATH = 'products_ice.wp76Qlxq/metadata/00001-c6f49b1a-d0e8-4a80-a136-a37612a60564.metadata.json';

-- ORDERS TABLE
CREATE OR REPLACE ICEBERG TABLE ext_orders
    EXTERNAL_VOLUME = 'tlv_datalake_s3_ev'
    CATALOG = 'tlv_iceberg_catalog_int'
    METADATA_FILE_PATH = 'orders_ice.nChXnlVD/metadata/00001-15afc5d0-80c3-4108-8918-bb2994c720d4.metadata.json';

-- ============================================================================
-- STEP 5: VERIFY - SCHEMA WAS AUTO-DETECTED!
-- ============================================================================
-- Notice we never defined columns - Snowflake read them from Iceberg metadata!

SHOW ICEBERG TABLES IN SCHEMA TLV_BUILD_HOL.EXTERNAL_ICEBERG;

DESC TABLE ext_customers;
DESC TABLE ext_products;
DESC TABLE ext_orders;

-- ============================================================================
-- STEP 6: QUERY THE DATA
-- ============================================================================
-- These queries read directly from S3 - no data was copied into Snowflake!

SELECT 'EXT_CUSTOMERS' as table_name, COUNT(*) as row_count FROM ext_customers
UNION ALL
SELECT 'EXT_PRODUCTS', COUNT(*) FROM ext_products
UNION ALL
SELECT 'EXT_ORDERS', COUNT(*) FROM ext_orders;

SELECT * FROM ext_customers LIMIT 5;
SELECT * FROM ext_products LIMIT 5;
SELECT * FROM ext_orders ORDER BY order_date DESC LIMIT 5;

-- Join across all tables
SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    c.segment,
    p.product_name,
    p.category,
    o.quantity,
    o.unit_price,
    o.order_date
FROM ext_orders o
JOIN ext_customers c ON o.customer_id = c.customer_id
JOIN ext_products p ON o.product_id = p.product_id
ORDER BY o.order_date DESC
LIMIT 10;

-- ============================================================================
-- KEY TAKEAWAYS
-- ============================================================================
/*
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WHY EXTERNALLY-MANAGED ICEBERG?                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ✅ NO DATA MOVEMENT    - Data stays in your S3/Azure/GCS bucket           │
│  ✅ NO ETL              - Zero copy, zero duplication                      │
│  ✅ MULTI-ENGINE        - Spark writes, Snowflake reads (or vice versa)    │
│  ✅ OPEN FORMAT         - No vendor lock-in, standard Iceberg/Parquet     │
│  ✅ AUTO SCHEMA         - Snowflake reads schema from metadata             │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SNOWFLAKE-MANAGED vs EXTERNALLY-MANAGED                                    │
│  ───────────────────────────────────────                                    │
│                                                                             │
│  SNOWFLAKE-MANAGED:                                                         │
│    • CATALOG = 'SNOWFLAKE'                                                  │
│    • Full DML (INSERT/UPDATE/DELETE)                                        │
│    • Snowflake manages metadata                                             │
│                                                                             │
│  EXTERNALLY-MANAGED (this script):                                          │
│    • CATALOG = catalog_integration + METADATA_FILE_PATH                     │
│    • Read-only in Snowflake                                                 │
│    • External engine manages metadata                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- OPTIONAL: REFRESH WHEN DATA CHANGES
-- ============================================================================
-- When Spark/Flink writes new data, they create a NEW metadata file.
-- To see the updates in Snowflake, refresh with the new path:
--
-- ALTER ICEBERG TABLE ext_orders REFRESH 
--     METADATA_FILE_PATH = 'orders_ice.xxx/metadata/00002-newfile.metadata.json';

-- ============================================================================
-- CLEANUP (if needed)
-- ============================================================================
/*
DROP ICEBERG TABLE IF EXISTS ext_orders;
DROP ICEBERG TABLE IF EXISTS ext_products;
DROP ICEBERG TABLE IF EXISTS ext_customers;
DROP CATALOG INTEGRATION IF EXISTS tlv_iceberg_catalog_int;
DROP EXTERNAL VOLUME IF EXISTS tlv_datalake_s3_ev;
DROP SCHEMA IF EXISTS TLV_BUILD_HOL.EXTERNAL_ICEBERG;
*/
