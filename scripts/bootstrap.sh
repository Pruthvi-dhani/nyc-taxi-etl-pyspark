#!/usr/bin/env bash
# bootstrap.sh
# EMR Serverless bootstrap action — installs Python dependencies from S3.
# Referenced in the EMR Serverless application configuration.
#
# Usage: uploaded to S3 and referenced as a bootstrap script in the
#        EMR Serverless application definition.

set -euo pipefail

BUCKET="${PIPELINE_BUCKET:-my-nyc-taxi-bucket}"
WHL_KEY="artifacts/nyc_taxi_etl_pyspark-1.0.0-py3-none-any.whl"

echo "[bootstrap] Installing wheel from s3://${BUCKET}/${WHL_KEY}"
aws s3 cp "s3://${BUCKET}/${WHL_KEY}" /tmp/nyc_taxi_etl.whl

pip install --quiet /tmp/nyc_taxi_etl.whl

echo "[bootstrap] Done."

