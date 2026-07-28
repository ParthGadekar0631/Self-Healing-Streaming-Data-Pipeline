#!/usr/bin/env bash
set -euo pipefail

COUNT="${1:-100}"
INVALID_RATE="${2:-0.10}"
exec python -m src.producer.kafka_producer \
  --count "$COUNT" \
  --invalid-rate "$INVALID_RATE"
