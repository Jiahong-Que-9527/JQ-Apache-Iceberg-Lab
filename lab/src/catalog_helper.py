from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal

from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

StorageBackend = Literal["minio", "seaweed"]


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _backend_config(backend: StorageBackend) -> dict[str, str]:
    if backend == "minio":
        return {
            "catalog_uri": _env("CATALOG_URI", "sqlite:///catalog.db"),
            "warehouse_bucket": _env("MINIO_BUCKET", "warehouse"),
            "s3_endpoint": _env("S3_ENDPOINT_MINIO", _env("S3_ENDPOINT", "http://minio:9000")),
            "access_key": _env("AWS_ACCESS_KEY_ID", "minioadmin"),
            "secret_key": _env("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "region": _env("AWS_REGION", "us-east-1"),
        }
    return {
        "catalog_uri": _env("CATALOG_URI_SEAWEED", "sqlite:///catalog_seaweed.db"),
        "warehouse_bucket": _env("SEAWEED_BUCKET", "warehouse"),
        "s3_endpoint": _env("S3_ENDPOINT_SEAWEED", "http://seaweedfs:8333"),
        "access_key": _env("SEAWEED_ACCESS_KEY_ID", "seaweedadmin"),
        "secret_key": _env("SEAWEED_SECRET_ACCESS_KEY", "seaweedadmin"),
        "region": _env("AWS_REGION", "us-east-1"),
    }


def storage_console_url(backend: StorageBackend = "minio") -> str:
    if backend == "minio":
        return "http://localhost:9001"
    return "http://localhost:9333"


def get_catalog(
    backend: StorageBackend = "minio",
    name: str | None = None,
) -> SqlCatalog:
    cfg = _backend_config(backend)
    catalog_name = name or f"lab_{backend}"
    catalog = SqlCatalog(
        catalog_name,
        uri=cfg["catalog_uri"],
        warehouse=f"s3://{cfg['warehouse_bucket']}/",
        **{
            "s3.endpoint": cfg["s3_endpoint"],
            "s3.access-key-id": cfg["access_key"],
            "s3.secret-access-key": cfg["secret_key"],
            "s3.region": cfg["region"],
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


def get_s3_client(backend: StorageBackend = "minio"):
    cfg = _backend_config(backend)
    return boto3.client(
        "s3",
        endpoint_url=cfg["s3_endpoint"],
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=cfg["region"],
        config=BotoConfig(s3={"addressing_style": "path"}),
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected an s3:// URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def download_s3_uri(
    uri: str,
    destination: str | Path,
    *,
    backend: StorageBackend = "minio",
) -> Path:
    bucket, key = parse_s3_uri(uri)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    get_s3_client(backend).download_file(bucket, key, str(destination))
    return destination


def read_json_s3(uri: str, *, backend: StorageBackend = "minio") -> dict[str, Any]:
    bucket, key = parse_s3_uri(uri)
    obj = get_s3_client(backend).get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def list_object_keys(
    prefix: str,
    *,
    backend: StorageBackend = "minio",
    bucket: str | None = None,
) -> list[str]:
    cfg = _backend_config(backend)
    bucket_name = bucket or cfg["warehouse_bucket"]
    client = get_s3_client(backend)
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append(item["Key"])
    return keys


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


def configure_duckdb_for_s3(con: Any, backend: StorageBackend = "minio") -> None:
    cfg = _backend_config(backend)
    endpoint = cfg["s3_endpoint"].replace("http://", "").replace("https://", "")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{cfg['access_key']}';")
    con.execute(f"SET s3_secret_access_key='{cfg['secret_key']}';")
    con.execute(f"SET s3_region='{cfg['region']}';")


def configure_duckdb_for_minio(con: Any) -> None:
    configure_duckdb_for_s3(con, "minio")
