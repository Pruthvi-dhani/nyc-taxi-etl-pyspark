#!/usr/bin/env bash
# submit_job.sh
# Submits a job to an existing EMR Serverless application via the AWS CLI.
#
# Usage:
#   bash scripts/submit_job.sh <job_type> <app_id> <exec_role_arn> <bucket> <region>
#
# job_type: ingest | transform | analytics
#
# Examples:
#   bash scripts/submit_job.sh ingest     $APP_ID $EXEC_ROLE_ARN my-bucket us-east-1
#   bash scripts/submit_job.sh transform  $APP_ID $EXEC_ROLE_ARN my-bucket us-east-1
#   bash scripts/submit_job.sh analytics  $APP_ID $EXEC_ROLE_ARN my-bucket us-east-1

set -euo pipefail

JOB_TYPE="${1:?Usage: submit_job.sh <ingest|transform|analytics> <app_id> <exec_role_arn> <bucket> <region>}"
APP_ID="${2:?Missing APP_ID}"
EXEC_ROLE_ARN="${3:?Missing EXEC_ROLE_ARN}"
BUCKET="${4:?Missing BUCKET}"
REGION="${5:-us-east-1}"

WHL_S3="s3://${BUCKET}/artifacts/nyc_taxi_etl_pyspark-1.0.0-py3-none-any.whl"
LOG_URI="s3://${BUCKET}/logs/emr-serverless/"

COMMON_SPARK_PARAMS=(
    "--conf" "spark.executor.cores=4"
    "--conf" "spark.executor.memory=16g"
    "--conf" "spark.driver.memory=8g"
    "--conf" "spark.sql.shuffle.partitions=400"
    "--conf" "spark.sql.adaptive.enabled=true"
    "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
    "--conf" "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--conf" "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog"
    "--conf" "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
    "--conf" "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--conf" "spark.sql.catalog.glue_catalog.warehouse=s3://${BUCKET}/iceberg-warehouse"
    "--py-files" "${WHL_S3}"
)

case "${JOB_TYPE}" in
    ingest)
        ENTRY_POINT="s3://${BUCKET}/scripts/ingest_job.py"
        ENTRY_ARGS='["--trip-type","yellow","--start-year","2022","--end-year","2023","--mode","append"]'
        JOB_NAME="nyc-taxi-ingest-$(date +%Y%m%d%H%M%S)"
        ;;
    transform)
        ENTRY_POINT="s3://${BUCKET}/scripts/transform_job.py"
        ENTRY_ARGS='["--trip-type","yellow","--mode","overwrite"]'
        JOB_NAME="nyc-taxi-transform-$(date +%Y%m%d%H%M%S)"
        ;;
    analytics)
        ENTRY_POINT="s3://${BUCKET}/scripts/analytics_job.py"
        ENTRY_ARGS='["--trip-type","yellow","--module","all"]'
        JOB_NAME="nyc-taxi-analytics-$(date +%Y%m%d%H%M%S)"
        ;;
    *)
        echo "ERROR: Unknown job type '${JOB_TYPE}'. Choose: ingest | transform | analytics"
        exit 1
        ;;
esac

echo "==> Submitting '${JOB_TYPE}' job to EMR Serverless application ${APP_ID}"

SPARK_PARAMS="${COMMON_SPARK_PARAMS[*]}"

JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --region "${REGION}" \
    --application-id "${APP_ID}" \
    --execution-role-arn "${EXEC_ROLE_ARN}" \
    --name "${JOB_NAME}" \
    --job-driver "{
        \"sparkSubmit\": {
            \"entryPoint\": \"${ENTRY_POINT}\",
            \"entryPointArguments\": ${ENTRY_ARGS},
            \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
        }
    }" \
    --configuration-overrides "{
        \"monitoringConfiguration\": {
            \"s3MonitoringConfiguration\": {
                \"logUri\": \"${LOG_URI}\"
            },
            \"cloudWatchLoggingConfiguration\": {
                \"enabled\": true,
                \"logGroupName\": \"/aws/emr-serverless/nyc-taxi-etl\",
                \"logStreamNamePrefix\": \"nyc-taxi\"
            }
        }
    }" \
    --query 'jobRunId' \
    --output text)

echo "==> Job submitted. Job Run ID: ${JOB_RUN_ID}"
echo "==> Monitor with:"
echo "    aws emr-serverless get-job-run --application-id ${APP_ID} --job-run-id ${JOB_RUN_ID} --region ${REGION}"

