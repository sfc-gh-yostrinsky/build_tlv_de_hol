#!/usr/bin/env bash
# =============================================================================
# AWS side of the TLV DE HOL raw lake: S3 bucket + Glue database + Lake Formation
# with the two-role vended-credentials pattern Snowflake uses for a Glue IRC CLD.
#
#   CATALOG role  (tlv-hol-snowflake-glue)  Snowflake assumes it (SigV4) to call
#                                           the Glue Iceberg REST API and ask Lake
#                                           Formation for vended S3 credentials.
#   DATA role     (tlv-hol-lf-data-access)  Lake Formation assumes it to read and
#                                           write the bucket on Snowflake's behalf.
#
# Idempotent: safe to re-run. The catalog role's trust policy is a placeholder
# until sql/01_glue_catalog_cld.sql has created the catalog integration; then run
#   ./aws/setup_glue_lakeformation.sh trust <GLUE_AWS_IAM_USER_ARN> <GLUE_AWS_EXTERNAL_ID>
# =============================================================================
set -euo pipefail

REGION="${AWS_REGION:-us-west-2}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${BUCKET:-tlv-hol-glue-iceberg-${ACCOUNT_ID}}"
GLUE_DB="${GLUE_DB:-tlv_hol_raw}"
CATALOG_ROLE="${CATALOG_ROLE:-tlv-hol-snowflake-glue}"
DATA_ROLE="${DATA_ROLE:-tlv-hol-lf-data-access}"
CATALOG_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${CATALOG_ROLE}"
DATA_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${DATA_ROLE}"
TAGS="Key=project,Value=tlv-de-hol Key=keep,Value=drop-after-hol"

# ---------------------------------------------------------------------------
# Subcommand: patch the catalog role trust policy with Snowflake's identity.
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "trust" ]]; then
  SF_ARN="$2"; SF_EXT_ID="$3"
  aws iam update-assume-role-policy --role-name "$CATALOG_ROLE" --policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
  "Principal":{"AWS":"${SF_ARN}"},"Action":"sts:AssumeRole",
  "Condition":{"StringEquals":{"sts:ExternalId":"${SF_EXT_ID}"}}}]}
JSON
)"
  echo "Trust policy on ${CATALOG_ROLE} now allows ${SF_ARN} (external id ${SF_EXT_ID})."
  exit 0
fi

echo "Account ${ACCOUNT_ID} / ${REGION} -> bucket ${BUCKET}, Glue DB ${GLUE_DB}"

# --- 1. S3 bucket ------------------------------------------------------------
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
fi
# New buckets block public access by default; some orgs' SCPs deny setting it explicitly.
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
  2>/dev/null || echo "Public access block not settable (SCP); relying on the S3 default (blocked)."
aws s3api put-bucket-tagging --bucket "$BUCKET" \
  --tagging 'TagSet=[{Key=project,Value=tlv-de-hol},{Key=keep,Value=drop-after-hol}]'

# --- 2. Glue database --------------------------------------------------------
aws glue get-database --name "$GLUE_DB" >/dev/null 2>&1 || \
  aws glue create-database --database-input \
    "{\"Name\":\"${GLUE_DB}\",\"Description\":\"TLV DE HOL raw lake (Snowflake CLD TLV_HOL_LAKE). Drop after HOL.\",\"LocationUri\":\"s3://${BUCKET}/${GLUE_DB}/\"}"

# --- 3. IAM roles ------------------------------------------------------------
# Catalog role: placeholder trust (this account) until Snowflake's identity exists.
aws iam get-role --role-name "$CATALOG_ROLE" >/dev/null 2>&1 || \
  aws iam create-role --role-name "$CATALOG_ROLE" --tags $TAGS \
    --description "Snowflake catalog role for TLV HOL Glue IRC CLD (SigV4 + LF vending)" \
    --assume-role-policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${ACCOUNT_ID}:root\"},\"Action\":\"sts:AssumeRole\"}]}" >/dev/null

# Glue API scoped to the one database (KNOWN_GOTCHAS #12), GetDataAccess must be "*".
aws iam put-role-policy --role-name "$CATALOG_ROLE" --policy-name glue-irc-and-lf --policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[
  {"Sid":"GlueIrc","Effect":"Allow","Action":["glue:Get*","glue:CreateTable","glue:UpdateTable","glue:DeleteTable","glue:BatchGet*"],
   "Resource":["arn:aws:glue:${REGION}:${ACCOUNT_ID}:catalog",
               "arn:aws:glue:${REGION}:${ACCOUNT_ID}:database/${GLUE_DB}",
               "arn:aws:glue:${REGION}:${ACCOUNT_ID}:table/${GLUE_DB}/*"]},
  {"Sid":"LakeFormationGetDataAccess","Effect":"Allow","Action":"lakeformation:GetDataAccess","Resource":"*"}]}
JSON
)"

# Data role: assumed by Lake Formation; read+write because Snowflake seeds the lake.
aws iam get-role --role-name "$DATA_ROLE" >/dev/null 2>&1 || \
  aws iam create-role --role-name "$DATA_ROLE" --tags $TAGS \
    --description "Lake Formation data access role for TLV HOL bucket" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lakeformation.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null

aws iam put-role-policy --role-name "$DATA_ROLE" --policy-name s3-rw --policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::${BUCKET}/*"},
  {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":"arn:aws:s3:::${BUCKET}"}]}
JSON
)"
sleep 10   # IAM propagation (KNOWN_GOTCHAS #6)

# --- 4. Lake Formation ---------------------------------------------------------
aws lakeformation register-resource --resource-arn "arn:aws:s3:::${BUCKET}" \
  --role-arn "$DATA_ROLE_ARN" 2>/dev/null || echo "S3 location already registered."

P="DataLakePrincipalIdentifier=${CATALOG_ROLE_ARN}"
aws lakeformation grant-permissions --principal "$P" \
  --permissions DESCRIBE CREATE_TABLE ALTER --resource "{\"Database\":{\"Name\":\"${GLUE_DB}\"}}"
aws lakeformation grant-permissions --principal "$P" \
  --permissions ALL --resource "{\"Table\":{\"DatabaseName\":\"${GLUE_DB}\",\"TableWildcard\":{}}}"
aws lakeformation grant-permissions --principal "$P" \
  --permissions DATA_LOCATION_ACCESS --resource "{\"DataLocation\":{\"ResourceArn\":\"arn:aws:s3:::${BUCKET}\"}}"

# Snowflake needs LF's external-engine full-table access. Account-wide, so only check it.
FULL=$(aws lakeformation get-data-lake-settings --query 'DataLakeSettings.AllowFullTableExternalDataAccess' --output text)
[[ "$FULL" == "True" ]] || { echo "ERROR: LF AllowFullTableExternalDataAccess is off (account-wide). Enable deliberately, then re-run."; exit 1; }

echo
echo "Done. Catalog role ARN for the Snowflake catalog integration:"
echo "  ${CATALOG_ROLE_ARN}"
