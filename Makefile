SHELL := /bin/bash

install: ## Install dependencies
	poetry install

lint: ## Linter
	export PIP_USER=0; poetry run pre-commit run --all-files

api-docs:  ## Generate rtd
	poetry install
	cd docs
	poetry run make html

check-license-header: ## Check if all *.py files have a license header
	curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash
	./bin/license-header-checker -a .github/license-header.txt . py

code-gen: ## Generate endpoints from OpenAPI specs
	mkdir -vp codegen/ws/
	rm -Rfv codegen/ws/*
	python codegen/generator.py
	cp -fv codegen/ws/*.py xata/api/.
	rm -Rfv codegen/ws/*.py

test: | unit-tests integration-tests ## Run unit & integration tests

unit-tests: ## Run unit tests
	poetry run pytest -v --tb=short tests/unit-tests/

unit-tests-cov: ## Unit tests coverage
	poetry run pytest --cov=xata tests/unit-tests

integration-tests: ## Run integration tests
	poetry run pytest --tb=short -W ignore::DeprecationWarning tests/integration-tests/

integration-tests-memray: ## Run integration tests with memray
	poetry run pytest --memray -W ignore::DeprecationWarning tests/integration-tests/

integration-tests-cov: ## Integration tests coverage
	poetry run pytest --cov=xata tests/integration-tests/

help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
#------------- <https://suva.sh/posts/well-documented-makefiles> --------------

.DEFAULT_GOAL := help
