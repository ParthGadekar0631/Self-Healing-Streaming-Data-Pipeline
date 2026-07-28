#!/usr/bin/env bash
set -euo pipefail

API_URL="${INCIDENT_API_URL:-http://localhost:8000}"
LIMIT="${1:-100}"
curl --fail-with-body \
  -X POST "${API_URL}/replay/quarantine" \
  -H "Content-Type: application/json" \
  -d "{\"limit\":${LIMIT},\"dry_run\":false}"
