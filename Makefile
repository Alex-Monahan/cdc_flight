SHELL := /bin/bash
PROJECT_DIR := $(shell cd "$(dir $(lastword $(MAKEFILE_LIST)))" && pwd)
UV ?= uv
PG := $(PROJECT_DIR)/scripts/pg.sh

# Everything (Postgres, the JVM, DuckDB) runs natively on the host: no Docker.
export PGHOST ?= 127.0.0.1
export PGPORT ?= 15432
export PGUSER ?= postgres
export PGPASSWORD ?= postgres
export PGDATABASE ?= cdc_source

.DEFAULT_GOAL := help

## ---------------------------------------------------------------------------
## environment
## ---------------------------------------------------------------------------

.PHONY: install
install: ## create .venv and install the project (+ dev extras)
	$(UV) venv --python 3.13
	$(UV) pip install -e ".[dev]"

## ---------------------------------------------------------------------------
## Postgres (project-local Homebrew cluster on :15432 - never touches :5432)
## ---------------------------------------------------------------------------

.PHONY: pg-init
pg-init: ## initdb a fresh cluster in ./.pgdata (wal_level=logical)
	$(PG) init

.PHONY: pg-start up
pg-start up: ## start the cluster and create the cdc_source database
	$(PG) start

.PHONY: pg-stop down
pg-stop down: ## stop the cluster
	$(PG) stop

.PHONY: pg-status
pg-status: ## show cluster status
	$(PG) status

.PHONY: pg-reset
pg-reset: ## destroy and recreate the cluster from scratch
	$(PG) reset

.PHONY: pg-psql
pg-psql: ## open psql against cdc_source
	$(PG) psql

.PHONY: seed
seed: ## (re)apply sql/01_schema.sql + sql/02_seed.sql
	$(PG) seed

.PHONY: reset
reset: clean-state pg-reset seed ## nuke everything (cluster, offsets, duckdb) and rebuild

## ---------------------------------------------------------------------------
## pipeline
## ---------------------------------------------------------------------------

.PHONY: changes
changes: ## generate one deterministic wave of inserts/updates/deletes
	$(UV) run cdc-datagen changes --scale 1 --seed 42

.PHONY: pipeline
pipeline: ## run the CDC pipeline into local DuckDB
	$(UV) run cdc-flight --destination duckdb --max-seconds 90 --idle-seconds 8

.PHONY: pipeline-fresh
pipeline-fresh: ## run the pipeline from scratch (drops offsets + dlt state first)
	$(UV) run cdc-flight --destination duckdb --reset-state --max-seconds 120 --idle-seconds 8

.PHONY: pipeline-md
pipeline-md: ## run the CDC pipeline into MotherDuck (needs $$motherduck_token)
	$(UV) run cdc-flight --destination motherduck --max-seconds 120 --idle-seconds 8

.PHONY: query
query: ## show what landed in the local DuckDB file
	$(UV) run python -m cdc_flight.inspect

## ---------------------------------------------------------------------------
## tests
## ---------------------------------------------------------------------------

.PHONY: test
test: ## run the default (local-only) suite with timings
	$(UV) run pytest -m "not motherduck" --durations=20

.PHONY: test-all
test-all: ## run everything including the MotherDuck smoke test
	$(UV) run pytest --durations=20

.PHONY: test-md
test-md: ## run only the MotherDuck tests
	$(UV) run pytest -m motherduck --durations=20

.PHONY: lint
lint: ## ruff
	$(UV) run ruff check src tests

## ---------------------------------------------------------------------------
## housekeeping
## ---------------------------------------------------------------------------

.PHONY: clean-state
clean-state: ## drop Debezium offsets, dlt pipeline state and the local DuckDB file
	rm -rf $(PROJECT_DIR)/.cdc_state $(PROJECT_DIR)/logs
	rm -f $(PROJECT_DIR)/cdc_flight.duckdb $(PROJECT_DIR)/cdc_flight.duckdb.wal

.PHONY: clean
clean: clean-state ## clean-state plus caches
	rm -rf $(PROJECT_DIR)/.pytest_cache $(PROJECT_DIR)/.ruff_cache
	find $(PROJECT_DIR)/src $(PROJECT_DIR)/tests -name __pycache__ -type d -exec rm -rf {} +

.PHONY: help
help:
	@grep -hE '^[a-zA-Z0-9_-]+([ ][a-zA-Z0-9_-]+)*:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
