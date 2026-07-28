#!/usr/bin/env bash
set -euo pipefail
exec python -m src.producer.kafka_producer --count "${1:-25}" --invalid-rate 1
