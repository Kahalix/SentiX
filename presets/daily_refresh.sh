#!/usr/bin/env bash
set -euo pipefail
KEYWORD="${1:?Usage: presets/daily_refresh.sh <keyword> [collection]}"
COLLECTION="${2:-$KEYWORD}"

python main.py --preset daily_refresh \
  --keyword "$KEYWORD" --collection "$COLLECTION"
