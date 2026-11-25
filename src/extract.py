"""Modular extractors for CSV, nested JSON, and Parquet sources."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import (
    ACTIVITY_LOGS_RAW_FILE,
    CUSTOMERS_RAW_FILE,
    ORDERS_RAW_FILE,
    get_logger,
)

logger = get_logger("extract")


@dataclass
class ExtractedSource:
    """Container holding extracted dataframe and source ingestion metadata."""

    source_name: str
    file_path: Path
    file_format: str
    row_count: int
    ingestion_timestamp: str
    dataframe: pd.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_csv(file_path: Path, source_name: str = "customers") -> ExtractedSource:
    """Read a CSV file into a pandas DataFrame with string preservation."""
    if not file_path.exists():
        raise FileNotFoundError(f"CSV source file not found: {file_path}")

    df = pd.read_csv(
        file_path,
        dtype=str,
        keep_default_na=True,
        na_values=["", "NULL", "null", "None", "N/A", "nan"],
    )
    ingestion_ts = _utc_now_iso()
    meta = {
        "filename": file_path.name,
        "file_path": str(file_path),
        "format": "csv",
        "row_count": len(df),
        "columns": list(df.columns),
        "ingestion_timestamp": ingestion_ts,
    }
    logger.info(
        "Extracted CSV source '%s': %d rows from %s",
        source_name,
        len(df),
        file_path.name,
    )
    return ExtractedSource(
        source_name=source_name,
        file_path=file_path,
        file_format="csv",
        row_count=len(df),
        ingestion_timestamp=ingestion_ts,
        dataframe=df,
        metadata=meta,
    )


def extract_json(file_path: Path, source_name: str = "orders") -> ExtractedSource:
    """Read nested JSON records, flattening shipping_address while preserving raw nested payloads."""
    if not file_path.exists():
        raise FileNotFoundError(f"JSON source file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as fh:
        content = fh.read().strip()

    if not content:
        raw_records: list[dict[str, Any]] = []
    elif content.startswith("["):
        raw_records = json.loads(content)
    else:
        raw_records = [json.loads(line) for line in content.splitlines() if line.strip()]

    flattened_rows: list[dict[str, Any]] = []
    for record in raw_records:
        row = dict(record)
        addr = row.get("shipping_address")
        if isinstance(addr, dict):
            row["shipping_street"] = addr.get("street")
            row["shipping_city"] = addr.get("city")
            row["shipping_state"] = addr.get("state")
            row["shipping_postal_code"] = addr.get("postal_code")
            row["shipping_country"] = addr.get("country")
        else:
            row["shipping_street"] = None
            row["shipping_city"] = None
            row["shipping_state"] = None
            row["shipping_postal_code"] = None
            row["shipping_country"] = None

        items = row.get("items")
        row["item_count"] = len(items) if isinstance(items, list) else 0
        flattened_rows.append(row)

    df = pd.DataFrame(flattened_rows)
    ingestion_ts = _utc_now_iso()
    meta = {
        "filename": file_path.name,
        "file_path": str(file_path),
        "format": "json",
        "row_count": len(df),
        "columns": list(df.columns),
        "ingestion_timestamp": ingestion_ts,
    }
    logger.info(
        "Extracted JSON source '%s': %d records from %s",
        source_name,
        len(df),
        file_path.name,
    )
    return ExtractedSource(
        source_name=source_name,
        file_path=file_path,
        file_format="json",
        row_count=len(df),
        ingestion_timestamp=ingestion_ts,
        dataframe=df,
        metadata=meta,
    )


def extract_parquet(file_path: Path, source_name: str = "activity_logs") -> ExtractedSource:
    """Read a Parquet file into a pandas DataFrame using PyArrow."""
    if not file_path.exists():
        raise FileNotFoundError(f"Parquet source file not found: {file_path}")

    df = pd.read_parquet(file_path, engine="pyarrow")
    ingestion_ts = _utc_now_iso()
    meta = {
        "filename": file_path.name,
        "file_path": str(file_path),
        "format": "parquet",
        "row_count": len(df),
        "columns": list(df.columns),
        "ingestion_timestamp": ingestion_ts,
    }
    logger.info(
        "Extracted Parquet source '%s': %d rows from %s",
        source_name,
        len(df),
        file_path.name,
    )
    return ExtractedSource(
        source_name=source_name,
        file_path=file_path,
        file_format="parquet",
        row_count=len(df),
        ingestion_timestamp=ingestion_ts,
        dataframe=df,
        metadata=meta,
    )


def extract_all(
    customers_path: Path = CUSTOMERS_RAW_FILE,
    orders_path: Path = ORDERS_RAW_FILE,
    activity_path: Path = ACTIVITY_LOGS_RAW_FILE,
) -> dict[str, ExtractedSource]:
    """Extract all configured raw sources and return a mapping by source name."""
    return {
        "customers": extract_csv(customers_path, source_name="customers"),
        "orders": extract_json(orders_path, source_name="orders"),
        "activity_logs": extract_parquet(activity_path, source_name="activity_logs"),
    }
