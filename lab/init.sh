#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created lab/.env from .env.example"
fi

mkdir -p warehouse-minio warehouse-seaweed notebooks src scripts seaweedfs
chmod a+rwx . warehouse-minio warehouse-seaweed notebooks src scripts seaweedfs

docker compose build jupyter
docker compose up -d minio seaweedfs
docker compose up bucket-init seaweed-bucket-init
docker compose up -d jupyter

echo
echo "Open http://localhost:8888 and start with notebooks/00_setup_check.ipynb"
echo "MinIO console: http://localhost:9001 (minioadmin / minioadmin)"
echo "SeaweedFS master UI: http://localhost:9333"
echo "SeaweedFS S3 API: http://localhost:8333 (seaweedadmin / seaweedadmin)"
