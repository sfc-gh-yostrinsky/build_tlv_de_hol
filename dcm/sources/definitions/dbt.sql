-- dbt project (dbt Fusion 2.0.0). Its models materialise as Snowflake-managed
-- Iceberg v3 tables (see models/marts/customer_lifetime_value.sql).
-- Early-access DCM capability: DEFINE DBT PROJECT takes the dbt folder under
-- sources/ directly in FROM (project assets aren't supported for dbt yet).
-- DEFINE DBT PROJECT rejects DBT_VERSION, so the version comes from the account
-- default: ALTER ACCOUNT SET DEFAULT_DBT_VERSION = '2.0.0' (see README).

DEFINE DBT PROJECT TLV_BUILD_HOL{{env_suffix}}.DATA_ENG_DEMO.DBT_ECOMMERCE
    FROM 'dbt_ecommerce'
    DEFAULT_TARGET = '{{dbt_target}}'
    COMMENT = 'TLV DE HOL customer lifetime value, Iceberg v3 ({{dbt_target}}). Managed by DCM.';
