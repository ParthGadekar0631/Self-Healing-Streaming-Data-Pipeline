PYTHON ?= python

.PHONY: install docker-up docker-down topics produce-events run-pipeline run-api simulate-failure replay-quarantine test clean

install:
	$(PYTHON) -m pip install -r requirements.txt

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

topics:
	docker compose run --rm topic-init

produce-events:
	$(PYTHON) -m src.producer.kafka_producer --count 100 --invalid-rate 0.10

run-pipeline:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 main.py pipeline

run-api:
	$(PYTHON) -m src.api.main

simulate-failure:
	bash scripts/simulate_failure.sh malformed

replay-quarantine:
	bash scripts/replay_quarantine.sh

test:
	pytest -q

clean:
	bash scripts/reset_environment.sh
