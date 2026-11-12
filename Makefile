# Shortcuts. Every target is a plain command you can also run by hand.
DATA ?= data
COMPOSE = docker compose -f local_pipeline/docker/docker-compose.yml

.PHONY: install lint test test-delta demo more show reset docker-up docker-down docker-logs

install:        ## Install everything needed to run and test locally
	pip install -r requirements.txt

lint:
	ruff check .
	ruff format --check .

test:           ## Fast tests (no downloads)
	pytest

test-delta:     ## End-to-end test on real Delta tables (downloads Delta on first run)
	pytest -m delta

demo:           ## Generate two hours of orders and run the whole pipeline once
	python -m ingestion.run_producer --target files --max-events 5000 --backfill-minutes 120 --landing-dir $(DATA)/landing/orders
	python -m local_pipeline.run_pipeline --source files --data-dir $(DATA)

more:           ## Add another batch and run again (shows incremental processing)
	python -m ingestion.run_producer --target files --max-events 1000 --backfill-minutes 20 --landing-dir $(DATA)/landing/orders
	python -m local_pipeline.run_pipeline --source files --data-dir $(DATA)

show:           ## Print a tour of the resulting tables
	python -m local_pipeline.show_results --data-dir $(DATA)

reset:          ## Delete all local data
	rm -rf $(DATA)

docker-up:      ## Kafka + producer + pipeline, continuously
	$(COMPOSE) up --build -d

docker-logs:
	$(COMPOSE) logs -f pipeline

docker-down:
	$(COMPOSE) down -v
