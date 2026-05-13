#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created lab/.env from .env.example"
fi

mkdir -p warehouse notebooks src
chmod a+rwx . warehouse notebooks src

docker compose build jupyter
docker compose up -d minio
docker compose up bucket-init
docker compose up -d jupyter

echo
echo "Open http://localhost:8888 and start with notebooks/00_setup_check.ipynb"
echo "MinIO console: http://localhost:9001"
echo "MinIO login: minioadmin / minioadmin"
