BUCKET         ?= my-nyc-taxi-bucket
REGION         ?= us-east-1
APP_ID         ?= $(shell aws emr-serverless list-applications --query 'applications[0].id' --output text)
EXEC_ROLE_ARN  ?= arn:aws:iam::123456789012:role/EMRServerlessExecutionRole
PACKAGE_NAME    = nyc_taxi_etl_pyspark
DIST_DIR        = dist

.PHONY: help install lint test build upload-artifacts submit-ingest submit-transform submit-analytics clean

help:
	@echo "Available targets:"
	@echo "  install           Install dependencies"
	@echo "  lint              Run ruff linter"
	@echo "  test              Run unit tests with coverage"
	@echo "  build             Build distributable wheel"
	@echo "  upload-artifacts  Upload wheel + job scripts to S3"
	@echo "  submit-ingest     Submit ingestion job to EMR Serverless"
	@echo "  submit-transform  Submit transformation job to EMR Serverless"
	@echo "  submit-analytics  Submit analytics job to EMR Serverless"
	@echo "  clean             Remove build artefacts"

install:
	pip install -e ".[dev]"

lint:
	ruff check src/ jobs/ tests/

test:
	pytest tests/unit/ --cov=src --cov-report=term-missing --cov-report=html

build:
	python -m build --wheel --outdir $(DIST_DIR)

upload-artifacts: build
	aws s3 cp $(DIST_DIR)/$(PACKAGE_NAME)-*.whl s3://$(BUCKET)/artifacts/
	aws s3 cp jobs/ingest_job.py      s3://$(BUCKET)/scripts/
	aws s3 cp jobs/transform_job.py   s3://$(BUCKET)/scripts/
	aws s3 cp jobs/analytics_job.py   s3://$(BUCKET)/scripts/
	aws s3 cp scripts/bootstrap.sh    s3://$(BUCKET)/scripts/

submit-ingest:
	bash scripts/submit_job.sh ingest $(APP_ID) $(EXEC_ROLE_ARN) $(BUCKET) $(REGION)

submit-transform:
	bash scripts/submit_job.sh transform $(APP_ID) $(EXEC_ROLE_ARN) $(BUCKET) $(REGION)

submit-analytics:
	bash scripts/submit_job.sh analytics $(APP_ID) $(EXEC_ROLE_ARN) $(BUCKET) $(REGION)

clean:
	rm -rf $(DIST_DIR) build *.egg-info .pytest_cache htmlcov .coverage

