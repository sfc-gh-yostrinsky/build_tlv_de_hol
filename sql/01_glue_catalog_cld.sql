-- ============================================================================
-- TLV DE HOL - PART 1: SHARED RAW LAKE (AWS Glue + Catalog-Linked Database)
-- ============================================================================
-- Run ONCE per account, before either DCM environment (DEV/PROD) is deployed.
-- Both environments read the same raw lake; only the transformation layer is
-- per-environment.
--
-- Prerequisite: ./aws/setup_glue_lakeformation.sh (bucket, Glue DB, IAM roles,
-- Lake Formation grants). Not managed by DCM: DCM cannot DEFINE catalog
-- integrations, catalog-linked databases or Iceberg tables.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- STEP 1: CATALOG INTEGRATION (Glue Iceberg REST, Lake Formation vended creds)
-- ----------------------------------------------------------------------------
-- No external volume anywhere: Lake Formation vends short-lived S3 credentials.
-- REST_CONFIG cannot be ALTERed; recreating it generates a NEW external ID, so
-- IF NOT EXISTS (not OR REPLACE) keeps the AWS trust policy valid on re-runs.
CREATE CATALOG INTEGRATION IF NOT EXISTS TLV_HOL_GLUE_CI
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'tlv_hol_raw'
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.us-west-2.amazonaws.com/iceberg'
    CATALOG_API_TYPE = AWS_GLUE
    CATALOG_NAME = '484577546576'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = 'arn:aws:iam::484577546576:role/tlv-hol-snowflake-glue'
    SIGV4_SIGNING_REGION = 'us-west-2'
  )
  ENABLED = TRUE
  COMMENT = 'TLV DE HOL raw lake: Glue IRC (tlv_hol_raw) with LF vended credentials, backs CLD TLV_HOL_LAKE. Drop after HOL. Reviewed 2026-10-05.';

-- Copy API_AWS_IAM_USER_ARN + API_AWS_EXTERNAL_ID from here into:
--   ./aws/setup_glue_lakeformation.sh trust <API_AWS_IAM_USER_ARN> <API_AWS_EXTERNAL_ID>
DESC CATALOG INTEGRATION TLV_HOL_GLUE_CI;

SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('TLV_HOL_GLUE_CI');

-- ----------------------------------------------------------------------------
-- STEP 2: CATALOG-LINKED DATABASE
-- ----------------------------------------------------------------------------
-- CASE_INSENSITIVE lets downstream SQL/dbt/Spark use unquoted names
-- (TLV_HOL_LAKE.tlv_hol_raw.orders). ALLOWED_WRITE_OPERATIONS = ALL is needed to
-- seed from Snowflake. WARNING: with writes on, DROP TABLE here also deletes the
-- table and its data in Glue/S3.
CREATE DATABASE IF NOT EXISTS TLV_HOL_LAKE
  LINKED_CATALOG = (
    CATALOG = 'TLV_HOL_GLUE_CI'
    ALLOWED_NAMESPACES = ('tlv_hol_raw')
    ALLOWED_WRITE_OPERATIONS = ALL
  )
  CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE
  COMMENT = 'TLV DE HOL shared raw lake: CLD over Glue tlv_hol_raw (vended creds). Read by DEV and PROD DCM envs. Drop after HOL. Reviewed 2026-10-05.';

SELECT SYSTEM$CATALOG_LINK_STATUS('TLV_HOL_LAKE');

-- ----------------------------------------------------------------------------
-- STEP 3: RAW TABLES (Iceberg v3, registered in Glue)
-- ----------------------------------------------------------------------------
-- Glue IRC needs an explicit BASE_LOCATION and does not support CTAS, so the
-- tables are created empty and seeded with INSERT below.
CREATE ICEBERG TABLE IF NOT EXISTS TLV_HOL_LAKE."tlv_hol_raw"."customers" (
  customer_id INT, first_name STRING, last_name STRING, email STRING,
  segment STRING, region STRING, created_at TIMESTAMP_NTZ(6)
) BASE_LOCATION = 's3://tlv-hol-glue-iceberg-484577546576/tlv_hol_raw/customers'
  ICEBERG_VERSION = 3
  COMMENT = 'TLV DE HOL raw customers (synthetic seed). Drop after HOL. Reviewed 2026-10-05.';

CREATE ICEBERG TABLE IF NOT EXISTS TLV_HOL_LAKE."tlv_hol_raw"."products" (
  product_id INT, product_name STRING, category STRING, subcategory STRING,
  cost_price NUMBER(10,2), list_price NUMBER(10,2)
) BASE_LOCATION = 's3://tlv-hol-glue-iceberg-484577546576/tlv_hol_raw/products'
  ICEBERG_VERSION = 3
  COMMENT = 'TLV DE HOL raw products (synthetic seed). Drop after HOL. Reviewed 2026-10-05.';

CREATE ICEBERG TABLE IF NOT EXISTS TLV_HOL_LAKE."tlv_hol_raw"."orders" (
  order_id INT, customer_id INT, product_id INT, quantity INT,
  unit_price NUMBER(10,2), discount_pct NUMBER(5,2),
  order_date TIMESTAMP_NTZ(6), status STRING
) BASE_LOCATION = 's3://tlv-hol-glue-iceberg-484577546576/tlv_hol_raw/orders'
  ICEBERG_VERSION = 3
  COMMENT = 'TLV DE HOL raw orders (synthetic seed). Drop after HOL. Reviewed 2026-10-05.';

-- ----------------------------------------------------------------------------
-- STEP 4: SEED SYNTHETIC DATA (only into empty tables, so re-runs are safe)
-- ----------------------------------------------------------------------------
INSERT INTO TLV_HOL_LAKE."tlv_hol_raw"."customers"
SELECT
  SEQ4() + 1,
  ARRAY_CONSTRUCT('Noa','Yael','Maya','Tamar','Avi','Omer','Itai','Lior','Dana','Eitan')[UNIFORM(0, 9, RANDOM())]::STRING,
  ARRAY_CONSTRUCT('Cohen','Levi','Mizrahi','Peretz','Biton','Friedman','Shapiro','Katz')[UNIFORM(0, 7, RANDOM())]::STRING,
  'customer' || (SEQ4() + 1) || '@example.com',
  ARRAY_CONSTRUCT('Consumer','SMB','Enterprise')[UNIFORM(0, 2, RANDOM())]::STRING,
  ARRAY_CONSTRUCT('North','Center','South','Jerusalem','Haifa')[UNIFORM(0, 4, RANDOM())]::STRING,
  DATEADD('day', -UNIFORM(30, 720, RANDOM()), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(6)
FROM TABLE(GENERATOR(ROWCOUNT => 300))
WHERE (SELECT COUNT(*) FROM TLV_HOL_LAKE."tlv_hol_raw"."customers") = 0;

INSERT INTO TLV_HOL_LAKE."tlv_hol_raw"."products"
WITH catalog AS (
  SELECT SEQ4() AS n,
         ARRAY_CONSTRUCT('Electronics','Home','Sports','Books','Fashion')[MOD(SEQ4(), 5)]::STRING AS category,
         ARRAY_CONSTRUCT(
           ARRAY_CONSTRUCT('Phones','Laptops','Audio'),
           ARRAY_CONSTRUCT('Kitchen','Furniture','Decor'),
           ARRAY_CONSTRUCT('Outdoor','Fitness','Cycling'),
           ARRAY_CONSTRUCT('Fiction','Tech','Kids'),
           ARRAY_CONSTRUCT('Shoes','Outerwear','Accessories'))[MOD(SEQ4(), 5)][MOD(FLOOR(SEQ4() / 5), 3)]::STRING AS subcategory,
         UNIFORM(10, 900, RANDOM())::NUMBER(10,2) AS list_price
  FROM TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT n + 1, subcategory || ' item ' || (n + 1), category, subcategory,
       ROUND(list_price * UNIFORM(45, 75, RANDOM()) / 100, 2), list_price
FROM catalog
WHERE (SELECT COUNT(*) FROM TLV_HOL_LAKE."tlv_hol_raw"."products") = 0;

INSERT INTO TLV_HOL_LAKE."tlv_hol_raw"."orders"
WITH o AS (
  SELECT SEQ4() + 1 AS order_id,
         UNIFORM(1, 300, RANDOM()) AS customer_id,
         UNIFORM(1, 50, RANDOM()) AS product_id,
         UNIFORM(1, 5, RANDOM()) AS quantity,
         ARRAY_CONSTRUCT(0, 0, 0, 5, 10, 15, 20)[UNIFORM(0, 6, RANDOM())]::NUMBER(5,2) AS discount_pct,
         DATEADD('minute', -UNIFORM(0, 90 * 24 * 60, RANDOM()), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(6) AS order_date,
         ARRAY_CONSTRUCT('COMPLETED','COMPLETED','COMPLETED','SHIPPED','SHIPPED','PROCESSING','PENDING','CANCELLED')[UNIFORM(0, 7, RANDOM())]::STRING AS status
  FROM TABLE(GENERATOR(ROWCOUNT => 5000))
)
SELECT o.order_id, o.customer_id, o.product_id, o.quantity, p.list_price,
       o.discount_pct, o.order_date, o.status
FROM o JOIN TLV_HOL_LAKE."tlv_hol_raw"."products" p ON p.product_id = o.product_id
WHERE (SELECT COUNT(*) FROM TLV_HOL_LAKE."tlv_hol_raw"."orders") = 0;

-- ----------------------------------------------------------------------------
-- STEP 5: VERIFY (also visible in AWS: aws glue get-tables --database-name tlv_hol_raw)
-- ----------------------------------------------------------------------------
SHOW ICEBERG TABLES IN DATABASE TLV_HOL_LAKE;

SELECT 'customers' AS t, COUNT(*) AS n FROM TLV_HOL_LAKE.tlv_hol_raw.customers
UNION ALL SELECT 'products', COUNT(*) FROM TLV_HOL_LAKE.tlv_hol_raw.products
UNION ALL SELECT 'orders',   COUNT(*) FROM TLV_HOL_LAKE.tlv_hol_raw.orders;
