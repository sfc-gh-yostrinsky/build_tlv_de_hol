#!/usr/bin/env bash
# Tear down the AWS side of the TLV DE HOL raw lake (after sql/99_cleanup.sql).
# Removes: Glue tables + database, Lake Formation grants/registration, S3 bucket
# (all objects), and the two IAM roles - identities last.
set -euo pipefail

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${BUCKET:-tlv-hol-glue-iceberg-${ACCOUNT_ID}}"
GLUE_DB="${GLUE_DB:-tlv_hol_raw}"
CATALOG_ROLE="${CATALOG_ROLE:-tlv-hol-snowflake-glue}"
DATA_ROLE="${DATA_ROLE:-tlv-hol-lf-data-access}"
CATALOG_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${CATALOG_ROLE}"
P="DataLakePrincipalIdentifier=${CATALOG_ROLE_ARN}"

read -r -p "Delete Glue DB ${GLUE_DB}, bucket ${BUCKET} (ALL data) and roles ${CATALOG_ROLE}/${DATA_ROLE}? [y/N] " ok
[[ "$ok" == "y" ]] || { echo "Aborted."; exit 1; }

aws lakeformation revoke-permissions --principal "$P" --permissions ALL \
  --resource "{\"Table\":{\"DatabaseName\":\"${GLUE_DB}\",\"TableWildcard\":{}}}" 2>/dev/null || true
aws lakeformation revoke-permissions --principal "$P" --permissions DESCRIBE CREATE_TABLE ALTER \
  --resource "{\"Database\":{\"Name\":\"${GLUE_DB}\"}}" 2>/dev/null || true
aws lakeformation revoke-permissions --principal "$P" --permissions DATA_LOCATION_ACCESS \
  --resource "{\"DataLocation\":{\"ResourceArn\":\"arn:aws:s3:::${BUCKET}\"}}" 2>/dev/null || true

for t in $(aws glue get-tables --database-name "$GLUE_DB" --query 'TableList[].Name' --output text 2>/dev/null); do
  aws glue delete-table --database-name "$GLUE_DB" --name "$t"
done
aws glue delete-database --name "$GLUE_DB" 2>/dev/null || true

aws lakeformation deregister-resource --resource-arn "arn:aws:s3:::${BUCKET}" 2>/dev/null || true
aws s3 rm "s3://${BUCKET}" --recursive --quiet || true
aws s3api delete-bucket --bucket "$BUCKET" 2>/dev/null || true

aws iam delete-role-policy --role-name "$CATALOG_ROLE" --policy-name glue-irc-and-lf 2>/dev/null || true
aws iam delete-role --role-name "$CATALOG_ROLE" 2>/dev/null || true
aws iam delete-role-policy --role-name "$DATA_ROLE" --policy-name s3-rw 2>/dev/null || true
aws iam delete-role --role-name "$DATA_ROLE" 2>/dev/null || true

echo "AWS side removed."
