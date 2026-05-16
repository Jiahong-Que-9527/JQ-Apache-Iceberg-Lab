#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ "${1:-}" != "--confirm" ]; then
  echo "This will delete lab warehouse data and SQLite catalogs."
  echo "  - warehouse-minio/ warehouse-seaweed/"
  echo "  - catalog.db catalog_seaweed.db"
  echo "Run: ./reset.sh --confirm"
  exit 1
fi

docker compose down --remove-orphans
rm -rf warehouse-minio warehouse-seaweed warehouse catalog.db catalog.db-journal catalog_seaweed.db catalog_seaweed.db-journal
bash init.sh
