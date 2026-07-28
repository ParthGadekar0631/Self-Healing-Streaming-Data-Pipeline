#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${PROJECT_ROOT}/data"

case "$DATA_ROOT" in
  "$PROJECT_ROOT"/data) ;;
  *) echo "Refusing unsafe data target: $DATA_ROOT" >&2; exit 1 ;;
esac

for directory in checkpoints parquet quarantine_exports metadata; do
  target="${DATA_ROOT}/${directory}"
  mkdir -p "$target"
  find "$target" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  touch "$target/.gitkeep"
done

echo "Removed generated checkpoints, Parquet files, quarantine exports, and metadata."
