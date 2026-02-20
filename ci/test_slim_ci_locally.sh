#!/bin/bash
# Test Slim CI locally
# Prerequisites: dbt_ecommerce project already deployed once to TLV_BUILD_HOL.DATA_ENG_DEMO

set -e

DBT_DATABASE="TLV_BUILD_HOL"
DBT_SCHEMA="DATA_ENG_DEMO"
DBT_PROJECT="dbt_ecommerce"

echo "=== Step 1: Get latest successful execution query_id ==="
QUERY_ID=$(snow sql -q "
SELECT QUERY_ID 
FROM TABLE($DBT_DATABASE.INFORMATION_SCHEMA.DBT_PROJECT_EXECUTION_HISTORY())
WHERE OBJECT_NAME = UPPER('$DBT_PROJECT')
  AND SCHEMA_NAME = UPPER('$DBT_SCHEMA')
  AND STATE = 'SUCCESS'
ORDER BY QUERY_START_TIME DESC
LIMIT 1" --format JSON | jq -r '.[0].QUERY_ID')

echo "Query ID: $QUERY_ID"

echo ""
echo "=== Step 2: Get artifact path ==="
ARTIFACT_PATH=$(snow sql -q "SELECT SYSTEM\$LOCATE_DBT_ARTIFACTS('$QUERY_ID')" --format JSON | jq -r '.[0] | to_entries | .[0].value')

echo "Artifact path: $ARTIFACT_PATH"

echo ""
echo "=== Step 3: Download production manifest into project ==="
mkdir -p ./dbt_ecommerce/prod_state
snow stage copy "${ARTIFACT_PATH}target/manifest.json" ./dbt_ecommerce/prod_state/ --overwrite
echo "Downloaded manifest.json to ./dbt_ecommerce/prod_state/"

echo ""
echo "=== Step 4: Deploy new version (with prod_state bundled) ==="
snow dbt deploy $DBT_PROJECT \
  --source ./dbt_ecommerce \
  --database $DBT_DATABASE \
  --schema $DBT_SCHEMA

echo ""
echo "=== Step 5: Run ONLY modified models (Slim CI) ==="
echo "This should only run new/changed models, not everything"
snow dbt execute \
  --database $DBT_DATABASE \
  --schema $DBT_SCHEMA \
  $DBT_PROJECT run \
  --state prod_state \
  --defer \
  --select "state:modified+"

echo ""
echo "=== Done! ==="
echo "If no models were modified, you should see 'Nothing to do'"
echo "If you added a new model, only that model should have run"
