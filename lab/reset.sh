#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ "${1:-}" != "--confirm" ]; then
  echo "This will delete lab/warehouse and lab/catalog.db."
  echo "Run: ./reset.sh --confirm"
  exit 1
fi

docker compose down --remove-orphans
rm -rf warehouse catalog.db catalog.db-journal
bash init.sh
