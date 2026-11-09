.PHONY: install run test cov lint format up down logs smoke bench async-up

install:            ## dev dependencies
	pip install -r requirements-dev.txt

run:                ## API with in-memory adapters (no Docker needed)
	uvicorn app.main:app --reload

test:
	pytest

cov:
	pytest --cov=app --cov-report=term-missing

lint:
	ruff check app tests scripts && ruff format --check app tests scripts

format:
	ruff format app tests scripts && ruff check --fix app tests scripts

up:                 ## full stack: gateway + 2 API replicas + worker + Elasticsearch + Redis
	docker compose up --build -d

async-up:           ## same stack with queued (202) indexing
	INDEXING_MODE=async docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f api indexer

smoke:
	bash scripts/smoke_test.sh http://localhost:8000

bench:              ## start the server with RATE_LIMIT_ENABLED=false first
	python scripts/benchmark.py --url http://localhost:8000 --out docs/benchmark-results.json
