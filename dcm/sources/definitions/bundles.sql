-- type:spark code bundle (PySpark on Snowpark Connect). One bundle body serves
-- both environments: the task passes the target database as an argument.

DEFINE CODE BUNDLE TLV_BUILD_HOL{{env_suffix}}.DATA_ENG_DEMO.CB_PRODUCT_ANALYSIS
    FROM 'asset://product_analysis'
    COMMENT = 'TLV DE HOL product category analysis, type:spark + PyPI dep ({{dbt_target}}). Managed by DCM.';
