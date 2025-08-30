#!/usr/bin/env bash
set -euo pipefail
KEYWORD="${1:?Usage: presets/server_headless.sh <keyword> [collection] [user_data_dir]}"
COLLECTION="${2:-$KEYWORD}"
USERDIR="${3:-}"

CMD=(python main.py --preset server_headless --keyword "$KEYWORD" --collection "$COLLECTION")
if [[ -n "$USERDIR" ]]; then CMD+=(--user-data-dir "$USERDIR"); fi
"${CMD[@]}"
