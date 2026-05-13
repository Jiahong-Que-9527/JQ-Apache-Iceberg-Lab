from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_catalog(name: str = "lab") -> SqlCatalog:
    catalog = SqlCatalog(
        name,
        uri=_env("CATALOG_URI", "sqlite:///catalog.db"),
        warehouse=f"s3://{_env('MINIO_BUCKET', 'warehouse')}/",
        **{
            "s3.endpoint": _env("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": _env("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": _env("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.region": _env("AWS_REGION", "us-east-1"),
            "s3.force-virtual-addressing": "false",
        },
    )
    catalog.create_tables()
    return catalog


def ensure_namespace(catalog: SqlCatalog, namespace: str = "lab") -> None:
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        return


def drop_table_if_exists(catalog: SqlCatalog, identifier: str) -> None:
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        return


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_env("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=_env("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=_env("AWS_REGION", "us-east-1"),
        config=BotoConfig(s3={"addressing_style": "path"}),
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected an s3:// URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def download_s3_uri(uri: str, destination: str | Path) -> Path:
    bucket, key = parse_s3_uri(uri)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    get_s3_client().download_file(bucket, key, str(destination))
    return destination


def read_json_s3(uri: str) -> dict[str, Any]:
    bucket, key = parse_s3_uri(uri)
    obj = get_s3_client().get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def current_metadata_location(table: Any) -> str:
    value = getattr(table, "metadata_location", None)
    if callable(value):
        value = value()
    if value:
        return str(value)

    metadata = getattr(table, "metadata", None)
    value = getattr(metadata, "metadata_location", None)
    if callable(value):
        value = value()
    if value:
        return str(value)

    raise RuntimeError("Could not determine the table metadata location.")


def configure_duckdb_for_minio(con: Any) -> None:
    endpoint = _env("S3_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{_env('AWS_ACCESS_KEY_ID', 'minioadmin')}';")
    con.execute(f"SET s3_secret_access_key='{_env('AWS_SECRET_ACCESS_KEY', 'minioadmin')}';")
    con.execute(f"SET s3_region='{_env('AWS_REGION', 'us-east-1')}';")
