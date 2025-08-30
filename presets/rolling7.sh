#!/usr/bin/env bash
set -euo pipefail
KEYWORD="${1:?Usage: presets/rolling7.sh <keyword> [collection] [max_tweets]}"
COLLECTION="${2:-$KEYWORD}"
MAXT="${3:-500}"

python main.py --preset rolling7 \
  --keyword "$KEYWORD" --collection "$COLLECTION" --max-tweets "$MAXT"
