#!/usr/bin/env python3
"""Create the warehouse bucket on MinIO or SeaweedFS if it does not exist."""

from __future__ import annotations

import os
import sys

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


def _ensure_bucket(
    *,
    endpoint: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(s3={"addressing_style": "path"}),
    )
    try:
        client.head_bucket(Bucket=bucket)
        print(f"Bucket already exists: {bucket} @ {endpoint}")
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket} @ {endpoint}")


def main() -> None:
    backend = (sys.argv[1] if len(sys.argv) > 1 else "seaweed").lower()
    region = os.environ.get("AWS_REGION", "us-east-1")

    if backend == "minio":
        _ensure_bucket(
            endpoint=os.environ.get("S3_ENDPOINT_MINIO", os.environ.get("S3_ENDPOINT", "http://minio:9000")),
            bucket=os.environ.get("MINIO_BUCKET", "warehouse"),
            access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            region=region,
        )
        return

    if backend == "seaweed":
        _ensure_bucket(
            endpoint=os.environ.get("S3_ENDPOINT_SEAWEED", "http://seaweedfs:8333"),
            bucket=os.environ.get("SEAWEED_BUCKET", "warehouse"),
            access_key=os.environ.get("SEAWEED_ACCESS_KEY_ID", "seaweedadmin"),
            secret_key=os.environ.get("SEAWEED_SECRET_ACCESS_KEY", "seaweedadmin"),
            region=region,
        )
        return

    raise SystemExit(f"Unknown backend: {backend}. Use 'minio' or 'seaweed'.")


if __name__ == "__main__":
    main()
