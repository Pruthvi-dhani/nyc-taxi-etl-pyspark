# NYC Taxi ETL & Analytics Pipeline

A production-grade **PySpark** pipeline on **Amazon EMR Serverless** that ingests NYC TLC trip data directly from S3, processes it through a Medallion architecture (Bronze → Silver → Gold), and produces rich analytics across five domains.

---

## Features

- **Medallion architecture** — Bronze (raw+audit), Silver (clean+enriched), Gold (pre-aggregated KPIs)
- **Iceberg + AWS Glue Catalog** — schema evolution, time-travel, ACID writes on Silver/Gold
- **EMR Serverless** — auto-scaling, serverless Spark with per-job cost model
- **Five analytics modules** — demand, fare, trip patterns, geo, time-series (20+ analytical outputs)
- **Pytest unit & integration tests** — in-memory fixtures, no S3 required for local testing
- **GitHub Actions CI** — lint (ruff) + unit tests on every push
- **One-command deploy** — `make upload-artifacts && make submit-ingest`

---

## Quick Start (Local)

### Prerequisites

- Python 3.10+
- Java 11 or 17 (required by PySpark 3.5.x)
- AWS credentials configured (for S3 access)

### Install

```bash
git clone https://github.com/your-org/nyc-taxi-etl-pyspark.git
cd nyc-taxi-etl-pyspark
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Run unit tests

```bash
make test
# or
pytest tests/unit/ -v
```

### Run the full pipeline locally (requires AWS credentials + S3 access)

```bash
python main.py \
  --trip-type yellow \
  --start-year 2022 \
  --end-year 2022 \
  --start-month 1 \
  --end-month 3 \
  --stage all
```

Run a single stage:

```bash
python main.py --trip-type yellow --start-year 2022 --end-year 2022 --stage ingest
```

---

## Deploy to EMR Serverless

### 1. Set environment variables

```bash
export PIPELINE_BUCKET=my-nyc-taxi-bucket
export AWS_REGION=us-east-1
export EMR_APP_ID=<your-emr-serverless-application-id>
export EXEC_ROLE_ARN=arn:aws:iam::123456789012:role/EMRServerlessExecutionRole
```

### 2. Create the EMR Serverless application (first time only)

```bash
aws emr-serverless create-application \
  --name nyc-taxi-etl \
  --release-label emr-7.1.0 \
  --type SPARK \
  --region $AWS_REGION
```

### 3. Build & upload artifacts

```bash
make upload-artifacts
```

This will:
- Build a `.whl` from `pyproject.toml`
- Upload the wheel + all job scripts to `s3://$PIPELINE_BUCKET/`

### 4. Submit jobs

```bash
make submit-ingest     # Raw TLC → Bronze
make submit-transform  # Bronze → Silver → Gold
make submit-analytics  # Gold → Analytics outputs
```

Or submit directly:

```bash
bash scripts/submit_job.sh ingest $EMR_APP_ID $EXEC_ROLE_ARN $PIPELINE_BUCKET $AWS_REGION
```

### 5. Monitor

```bash
aws emr-serverless get-job-run \
  --application-id $EMR_APP_ID \
  --job-run-id <job-run-id> \
  --region $AWS_REGION
```

Logs are streamed to `s3://$PIPELINE_BUCKET/logs/emr-serverless/` and CloudWatch Logs at `/aws/emr-serverless/nyc-taxi-etl`.

---

## S3 Bucket Layout

```
s3://my-nyc-taxi-bucket/
├── artifacts/           ← wheel file
├── scripts/             ← job entrypoint .py files
├── bronze/yellow/       ← raw partitioned by year/month
├── silver/yellow/       ← clean partitioned by pickup_year/pickup_month
├── gold/
│   ├── trips_by_hour/
│   ├── trips_by_date/
│   ├── trips_by_borough/
│   ├── fare_summary/
│   └── trip_duration_summary/
├── analytics/
│   ├── demand/
│   ├── fare/
│   ├── trip_patterns/
│   ├── geo/
│   └── time_series/
├── reference/
│   └── taxi_zone_lookup.csv
├── iceberg-warehouse/
└── logs/emr-serverless/
```

---

## Configuration

All paths and runtime parameters are controlled via environment variables (see `src/config.py`):

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_BUCKET` | `my-nyc-taxi-bucket` | Your S3 bucket |
| `RAW_BUCKET` | `nyc-tlc` | TLC public source bucket |
| `START_YEAR` / `END_YEAR` | `2022` / `2023` | Date range for ingestion |
| `START_MONTH` / `END_MONTH` | `1` / `12` | Month range |
| `DATABASE_NAME` | `nyc_taxi` | Glue database name |
| `LOG_LEVEL` | `INFO` | Python log level |
| `SPARK_EXECUTOR_MEMORY` | `16g` | Per-executor memory |
| `SHUFFLE_PARTITIONS` | `400` | `spark.sql.shuffle.partitions` |

---

## Analytics Outputs

| Domain | Outputs |
|---|---|
| **Demand** | Hourly demand profile, daily trend (7-day rolling avg), borough breakdown, MoM change |
| **Fare** | Fare by distance bucket, tip % distribution, payment type trend, top revenue zones, anomaly detection |
| **Trip Patterns** | Top corridors, airport trip share, distance by borough, busiest zones, weekday vs. weekend hourly |
| **Geo** | Zone trip volume (choropleth-ready), revenue per mile, inter-borough OD matrix, net flow |
| **Time-Series** | Monthly trend + YoY, COVID impact & recovery, seasonal index, fare evolution YoY |

---

## Development

```bash
make lint    # ruff check
make test    # pytest with coverage
make build   # build wheel
```

---

## Documentation

- [`DESIGN.md`](DESIGN.md) — architecture, data layer definitions, EMR deployment design, analytics catalogue



