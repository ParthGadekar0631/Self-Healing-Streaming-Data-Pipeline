#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${1:-${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}}"
KAFKA_TOPICS="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

if [[ ! -x "$KAFKA_TOPICS" ]]; then
  KAFKA_TOPICS="kafka-topics.sh"
fi

for topic in \
  events.raw events.validated events.transformed events.aggregated \
  events.quarantine events.dead_letter events.replay
do
  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list
