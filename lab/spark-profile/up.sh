#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! docker network inspect jq-apache-iceberg-lab_default >/dev/null 2>&1; then
  echo "Main lab network not found. Start it first:" >&2
  echo "  cd .. && ./init.sh" >&2
  exit 1
fi

mkdir -p state/lakekeeper-pg

docker compose up -d lakekeeper-db
docker compose run --rm lakekeeper-migrate
docker compose up -d lakekeeper
docker compose run --rm lakekeeper-bootstrap || true
docker compose up -d spark-iceberg trino

echo
echo "Spark profile up."
echo "  Lakekeeper UI:  http://localhost:8181"
echo "  Trino UI:       http://localhost:8090"
echo "  Spark shell:    docker compose exec spark-iceberg spark-sql"
echo "  Trino shell:    docker compose exec trino trino"
