#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

docker compose down --remove-orphans

echo "Spark profile stopped."
echo "State (Postgres volume) preserved at: ./state/"
echo "To wipe state too: rm -rf state/"
