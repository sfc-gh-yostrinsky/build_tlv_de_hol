{{
    config(
        materialized='table',
        tags=['marts', 'test']
    )
}}

SELECT 
    1 as test_id,
    'slim_ci_test' as test_name,
    CURRENT_TIMESTAMP() as created_at
