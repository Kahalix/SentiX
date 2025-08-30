#!/usr/bin/env bash
set -euo pipefail
KEYWORD="${1:?Usage: presets/deep_crawl.sh <keyword> <since> <until> [collection] [max_tweets]}"
SINCE="${2:?Pass YYYY-MM-DD}"
UNTIL="${3:?Pass YYYY-MM-DD}"
COLLECTION="${4:-$KEYWORD}"
MAXT="${5:-5000}"

python main.py --preset deep_crawl \
  --keyword "$KEYWORD" --since "$SINCE" --until "$UNTIL" \
  --collection "$COLLECTION" --max-tweets "$MAXT"
