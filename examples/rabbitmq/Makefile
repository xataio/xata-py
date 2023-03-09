SHELL := /bin/bash

run: ## Run the system
	docker-compose --compatibility up --no-deps --build

clean: ## Run Example
	docker-compose rm --stop --force xata_examples_rabbitmq xata_examples_producer xata_examples_consumers
	docker-compose rm --force --volumes xata_examples_rabbitmq xata_examples_producer xata_examples_consumers

setup: ## Setup Xata for running the example
	pip3 install -r requirements.txt
	python3 install.py

help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
#------------- <https://suva.sh/posts/well-documented-makefiles> --------------

.DEFAULT_GOAL := help
