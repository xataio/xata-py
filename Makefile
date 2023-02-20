SHELL := /bin/bash

install: ## Install dependencies
	poetry install

lint: ## Linter
	export PIP_USER=0; poetry run pre-commit run --all-files

api-docs: ## Generate the API documentation
	mkdir -vp api-docs && rm -Rfv api-docs/*
	poetry run pdoc3 --html -o api-docs/. xata/.

check-license-header: ## Check if all *.py files have a license header
	curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash
	./bin/license-header-checker -a .github/license-header.txt . py

code-gen: ## Generate endpoints from OpenAPI specs
	mkdir -vp codegen/ws/$(scope)
	rm -Rfv codegen/ws/$(scope)/*
	python codegen/generator.py --scope=$(scope)

code-gen-copy: ## Copy generated endpoints to target dir
	cp -fv codegen/ws/$(scope)/*.py xata/namespaces/$(scope)/.

test: | unit-tests integration-tests ## Run unit & integration tests

unit-tests: ## Run unit tests
	poetry run pytest -v --tb=short tests/unit-tests/

unit-tests-cov: ## Unit tests coverage
	poetry run pytest --cov=xata tests/unit-tests

integration-tests: ## Run integration tests
	poetry run pytest -v --tb=short tests/integration-tests/

integration-tests-cov: ## Integration tests coverage
	poetry run pytest --cov=xata tests/integration-tests/

help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
#------------- <https://suva.sh/posts/well-documented-makefiles> --------------

.DEFAULT_GOAL := help
