# AWS Commerce Intelligence Platform - Makefile
# Common commands for development workflow

.PHONY: help setup setup-dev test test-unit test-integration test-regression test-performance lint lint-fix terraform-init terraform-plan terraform-apply redpanda-start redpanda-stop flink-start flink-stop prefect-start grafana-start generator-start api-start clean

help:
	@echo "AWS Commerce Intelligence Platform - Available Commands"
	@echo "======================================================="
	@echo ""
	@echo "SETUP"
	@echo "  setup              - Install Python dependencies"
	@echo "  setup-dev          - Install with development dependencies"
	@echo ""
	@echo "TESTING"
	@echo "  test               - Run all tests"
	@echo "  test-unit          - Run unit tests only"
	@echo "  test-integration   - Run integration tests only"
	@echo "  test-regression    - Run regression tests only"
	@echo "  test-performance   - Run performance tests only"
	@echo ""
	@echo "CODE QUALITY"
	@echo "  lint               - Run ruff linter"
	@echo "  lint-fix           - Auto-fix ruff errors"
	@echo ""
	@echo "TERRAFORM"
	@echo "  terraform-init     - Initialise Terraform"
	@echo "  terraform-plan     - Show Terraform execution plan"
	@echo "  terraform-apply    - Apply Terraform changes to AWS"
	@echo "  terraform-destroy  - Destroy all Terraform-managed AWS resources"
	@echo ""
	@echo "SERVICES"
	@echo "  redpanda-start     - Start Redpanda broker"
	@echo "  redpanda-stop      - Stop Redpanda broker"
	@echo "  flink-start        - Start Flink in local mode"
	@echo "  flink-stop         - Stop Flink"
	@echo "  prefect-start      - Start Prefect agent"
	@echo "  grafana-start      - Start Grafana"
	@echo "  generator-start    - Start data generator (all domains)"
	@echo "  api-start          - Start FastAPI server"
	@echo ""
	@echo "UTILITIES"
	@echo "  clean              - Remove cache and artifacts"

# ====================================================================
# SETUP
# ====================================================================

setup:
	python3 -m pip install --break-system-packages -r requirements.txt

setup-dev:
	python3 -m pip install --break-system-packages -e .[dev]

# ====================================================================
# TESTING
# ====================================================================

test:
	PYTHONPATH=$(PWD) pytest tests/ -v

test-unit:
	PYTHONPATH=$(PWD) pytest tests/generator/ tests/flink/ tests/api/ -v \
		--cov=src --cov=fastapi \
		--cov-report=term-missing \
		--cov-report=html

test-integration:
	PYTHONPATH=$(PWD) pytest tests/integration/ -v

test-regression:
	PYTHONPATH=$(PWD) pytest tests/ -m regression -v

test-performance:
	PYTHONPATH=$(PWD) pytest tests/ -m performance -v

# ====================================================================
# CODE QUALITY
# ====================================================================

lint:
	ruff check . --exclude data/ --exclude docs/ --exclude .terraform/

lint-fix:
	ruff check --fix .
	ruff check --fix --unsafe-fixes .

# ====================================================================
# TERRAFORM
# ====================================================================

terraform-init:
	cd terraform && terraform init

terraform-plan:
	cd terraform && terraform plan

terraform-apply:
	cd terraform && terraform apply

terraform-destroy:
	cd terraform && terraform destroy

# ====================================================================
# SERVICES
# ====================================================================

redpanda-start:
	rpk redpanda start --overprovisioned --smp 1 --memory 200M --reserve-memory 0M &

redpanda-stop:
	rpk redpanda stop

flink-start:
	$(FLINK_HOME)/bin/start-cluster.sh

flink-stop:
	$(FLINK_HOME)/bin/stop-cluster.sh

prefect-start:
	PYTHONPATH=$(PWD) prefect agent start -q default

grafana-start:
	grafana-server --homepath /usr/share/grafana &

generator-start:
	PYTHONPATH=$(PWD) python3 src/generator/main.py

api-start:
	PYTHONPATH=$(PWD) uvicorn fastapi.main:app --host 0.0.0.0 --port 8000 --reload

# ====================================================================
# UTILITIES
# ====================================================================

clean:
	rm -rf .coverage coverage.xml htmlcov/
	rm -rf .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
