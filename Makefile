.PHONY: env key bootstrap verify up down logs seed producer replay test lint

env:
	@test -f .env || cp .env.example .env

key:
	python scripts/generate_keypair.py

bootstrap:
	python scripts/bootstrap_snowflake.py

verify:
	python scripts/verify_setup.py

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

seed:
	python scripts/generate_seed.py

producer:
	python -m kafka.producer.waqi_producer

replay:
	python -m kafka.producer.replay_producer

test:
	pytest -q

lint:
	ruff check .
	sqlfluff lint snowflake/sql
