#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-malformed}"

case "$MODE" in
  malformed)
    python -m src.producer.kafka_producer --count 50 --invalid-rate 1
    ;;
  redis)
    docker compose pause redis
    echo "Redis paused. Run '$0 restore' to resume it."
    ;;
  kafka)
    docker compose stop kafka
    echo "Kafka stopped. Run '$0 restore' to restart dependencies."
    ;;
  checkpoint)
    mkdir -p data/checkpoints/clean
    touch data/checkpoints/clean/_CORRUPT
    echo "Created the explicit clean-query corruption marker."
    ;;
  restore)
    docker compose unpause redis 2>/dev/null || true
    docker compose start kafka redis
    rm -f data/checkpoints/clean/_CORRUPT
    ;;
  *)
    echo "Usage: $0 {malformed|redis|kafka|checkpoint|restore}" >&2
    exit 2
    ;;
esac
