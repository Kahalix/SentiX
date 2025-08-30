#!/usr/bin/env bash
set -euo pipefail
KEYWORD="${1:?Usage: presets/db_only_analysis.sh <keyword> [collection] [since] [until] [max_tweets]}"
COLLECTION="${2:-$KEYWORD}"
SINCE="${3:-}"
UNTIL="${4:-}"
MAXT="${5:-1000}"

EXTRA=()
if [[ -n "$SINCE" ]]; then EXTRA+=(--since "$SINCE"); fi
if [[ -n "$UNTIL" ]]; then EXTRA+=(--until "$UNTIL"); fi

python main.py --preset db_only \
  --keyword "$KEYWORD" --collection "$COLLECTION" --max-tweets "$MAXT" "${EXTRA[@]}"
