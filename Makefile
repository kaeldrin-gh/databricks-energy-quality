PYTHON ?= python

.PHONY: help test lint fmt validate deploy run publish clean

help: ## show available targets
	@echo "targets: test lint fmt validate deploy run publish clean"

test: ## run the unit tests (no workspace needed)
	$(PYTHON) -m pytest -q

lint: ## ruff check + format check
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

fmt: ## ruff format
	$(PYTHON) -m ruff format .

validate: ## validate the bundle (needs databricks auth)
	databricks bundle validate -t free

deploy: ## deploy the bundle to the Free Edition workspace
	databricks bundle deploy -t free

run: ## run the daily job once
	databricks bundle run -t free energy_quality_job

publish: ## publish the deployed dashboard (viewers see the published revision)
	$(PYTHON) scripts/publish_dashboard.py -t free

clean: ## remove local build and cache artifacts
	$(PYTHON) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('dist', 'build', '.pytest_cache', '.ruff_cache')]"
