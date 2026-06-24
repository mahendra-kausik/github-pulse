.PHONY: setup tf-apply tf-plan ingest backfill dbt up down lint test clean help
.DEFAULT_GOAL := help

# Load .env if present so DATE/dataset vars are available to recipes.
ifneq (,$(wildcard .env))
include .env
export
endif

PYTHON ?= python
VENV   ?= .venv
DAYS   ?= 7

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup:  ## Create venv and install dependencies
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt

tf-plan:  ## terraform plan
	cd terraform && terraform init && terraform plan

tf-apply:  ## terraform init + apply (creates bucket, datasets, service account)
	cd terraform && terraform init && terraform apply

ingest:  ## Ingest a single day: make ingest DATE=2024-01-01
	@test -n "$(DATE)" || (echo "ERROR: set DATE=YYYY-MM-DD" && exit 1)
	$(PYTHON) -m ingestion.download   --date $(DATE)
	$(PYTHON) -m ingestion.transform  --date $(DATE)
	$(PYTHON) -m ingestion.upload_gcs --date $(DATE)
	$(PYTHON) -m ingestion.load_bq    --date $(DATE)

backfill:  ## Ingest a window: make backfill START=2024-01-01 DAYS=7
	@test -n "$(START)" || (echo "ERROR: set START=YYYY-MM-DD" && exit 1)
	$(PYTHON) -m ingestion.backfill --start $(START) --days $(DAYS)

dbt:  ## Run dbt build (staging + marts + tests)
	cd dbt && dbt deps && dbt build

up:  ## Start Kestra (orchestration)
	docker compose -f orchestration/docker-compose.yml up -d

down:  ## Stop Kestra
	docker compose -f orchestration/docker-compose.yml down

lint:  ## ruff + sqlfluff
	ruff check .
	sqlfluff lint dbt/models

test:  ## Run pytest
	pytest

clean:  ## Remove local data scratch + dbt target
	rm -rf $(DATA_DIR) dbt/target dbt/dbt_packages dbt/logs
