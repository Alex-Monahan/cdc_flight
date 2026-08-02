SHELL := /bin/bash
PROJECT_DIR := $(shell cd "$(dir $(lastword $(MAKEFILE_LIST)))" && pwd)
UV ?= uv
PG := $(PROJECT_DIR)/scripts/pg.sh
RUNTIME_STATE := $(PROJECT_DIR)/scripts/runtime_state.sh

# The test cluster is deliberately separate from any Postgres service on the
# host.  Use CDC_TEST_PGPORT as the one required per-instance input; all other
# instance-scoped paths and names derive from it unless explicitly overridden.
CDC_TEST_PGPORT ?= 15432
CDC_TEST_PGDATA ?= $(PROJECT_DIR)/.pgdata$(if $(filter 15432,$(CDC_TEST_PGPORT)),,_$(CDC_TEST_PGPORT))
CDC_TEST_INSTANCE_ID ?= pg$(CDC_TEST_PGPORT)
CDC_TEST_PGSOCKET ?= $(CDC_TEST_PGDATA)
CDC_TEST_PGLOG ?= $(CDC_TEST_PGDATA)/server.log
CDC_TEST_LOCK_PATH ?= $(PROJECT_DIR)/.pytest-source-$(CDC_TEST_INSTANCE_ID).lock
CDC_TEST_SETUP_LOCK_PATH ?= $(PROJECT_DIR)/.pytest-source-$(CDC_TEST_INSTANCE_ID)-setup.lock
CDC_TEST_PGDATABASE ?= cdc_source
CDC_TEST_TEMPLATE_DATABASE_PREFIX ?= cdc_flight_test_template_$(CDC_TEST_INSTANCE_ID)_
CDC_TEST_WORKER_DATABASE_PREFIX ?= cdc_flight_test_$(CDC_TEST_INSTANCE_ID)_
CDC_TEST_SLOT_PREFIX ?= test_slot_$(CDC_TEST_INSTANCE_ID)_
CDC_STATE_DIR ?= $(PROJECT_DIR)/.cdc_instances/$(CDC_TEST_INSTANCE_ID)/cdc_state
CDC_PIPELINES_DIR ?= $(CDC_STATE_DIR)/dlt_pipelines
CDC_DUCKDB_PATH ?= $(PROJECT_DIR)/.cdc_instances/$(CDC_TEST_INSTANCE_ID)/cdc_flight.duckdb
CDC_PIPELINE_NAME ?= cdc_flight_$(CDC_TEST_INSTANCE_ID)

# Everything (Postgres, the JVM, DuckDB) runs natively on the host: no Docker.
export PGHOST ?= 127.0.0.1
export CDC_TEST_PGPORT
export CDC_TEST_PGDATA
export CDC_TEST_INSTANCE_ID
export CDC_TEST_PGSOCKET
export CDC_TEST_PGLOG
export CDC_TEST_LOCK_PATH
export CDC_TEST_SETUP_LOCK_PATH
export CDC_TEST_PGDATABASE
export CDC_TEST_TEMPLATE_DATABASE_PREFIX
export CDC_TEST_WORKER_DATABASE_PREFIX
export CDC_TEST_SLOT_PREFIX
export CDC_STATE_DIR
export CDC_PIPELINES_DIR
export CDC_DUCKDB_PATH
export CDC_PIPELINE_NAME
export PGPORT = $(CDC_TEST_PGPORT)
export PGDATA = $(CDC_TEST_PGDATA)
export PGUSER ?= postgres
export PGPASSWORD ?= postgres
export PGDATABASE = $(CDC_TEST_PGDATABASE)
# PyArrow 25.0.0's mimalloc pool can SIGSEGV when Arrow arrays are built on the
# JPype/JVM callback thread. This is a production compatibility requirement too;
# keep every repository launch safe while respecting an explicit operator choice.
export ARROW_DEFAULT_MEMORY_POOL ?= system
PYTEST_WORKERS ?= 12
PYTEST_XDIST_ARGS ?= -n $(PYTEST_WORKERS) --dist=loadscope --max-worker-restart=0

.DEFAULT_GOAL := help

## ---------------------------------------------------------------------------
## environment
## ---------------------------------------------------------------------------

.PHONY: install
install: ## create .venv and install the project (+ dev extras)
	$(UV) venv --python 3.13
	$(UV) pip install -e ".[dev]"

## ---------------------------------------------------------------------------
## Postgres (project-local Homebrew cluster on the CDC_TEST_PGPORT - never touches :5432)
## ---------------------------------------------------------------------------

.PHONY: pg-init
pg-init: ## initdb a fresh cluster in the instance data directory (wal_level=logical)
	$(PG) init

.PHONY: pg-start up
pg-start up: ## start the instance cluster and create its test database
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
pg-psql: ## open psql against the instance test database
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

.PHONY: prepare-state
prepare-state: ## mark the selected project-local runtime directory as disposable
	$(RUNTIME_STATE) prepare

.PHONY: pipeline
pipeline: prepare-state ## run the CDC pipeline into local DuckDB
	$(UV) run cdc-flight --destination duckdb --max-seconds 90 --idle-seconds 8

.PHONY: pipeline-fresh
pipeline-fresh: prepare-state ## run the pipeline from scratch (drops offsets + dlt state first)
	$(UV) run cdc-flight --destination duckdb --reset-state --max-seconds 120 --idle-seconds 8

.PHONY: pipeline-md
pipeline-md: prepare-state ## run the CDC pipeline into MotherDuck (needs $$motherduck_token)
	$(UV) run cdc-flight --destination motherduck --max-seconds 120 --idle-seconds 8

.PHONY: query
query: ## show what landed in the local DuckDB file
	$(UV) run python -m cdc_flight.inspect

## ---------------------------------------------------------------------------
## tests
## ---------------------------------------------------------------------------

.PHONY: test
test: ## run the default local-only suite in parallel (12 workers by default)
	$(UV) run pytest $(PYTEST_XDIST_ARGS) -m "not motherduck and not slow" --durations=20

.PHONY: test-serial
test-serial: ## run the default local-only suite serially for debugging
	$(UV) run pytest -p no:xdist -m "not motherduck and not slow" --durations=20

.PHONY: test-all
test-all: ## run everything: MotherDuck smoke test + slow fault injection
	$(UV) run pytest --durations=20

.PHONY: test-md
test-md: ## run only the MotherDuck tests
	$(UV) run pytest -m motherduck --durations=20

.PHONY: test-slow
test-slow: ## run only the slow fault-injection tests (real SIGKILL, big loads)
	$(UV) run pytest -m "slow and not motherduck" --durations=20

.PHONY: lint
lint: ## ruff
	$(UV) run ruff check src tests

## ---------------------------------------------------------------------------
## housekeeping
## ---------------------------------------------------------------------------

.PHONY: clean-state
clean-state: ## drop Debezium offsets, dlt pipeline state and the local DuckDB file
	$(RUNTIME_STATE) clean

.PHONY: clean
clean: clean-state ## clean-state plus caches
	rm -rf $(PROJECT_DIR)/.pytest_cache $(PROJECT_DIR)/.ruff_cache
	find $(PROJECT_DIR)/src $(PROJECT_DIR)/tests -name __pycache__ -type d -exec rm -rf {} +

.PHONY: help
help:
	@grep -hE '^[a-zA-Z0-9_-]+([ ][a-zA-Z0-9_-]+)*:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
