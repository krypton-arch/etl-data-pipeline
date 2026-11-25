"""Data transformation: deduplication, standardization, enrichment, and PII masking."""

from __future__ import annotations

import hashlib
import re
from typing import Any

import pandas as pd

from config.settings import PII_HASH_SALT, get_logger

logger = get_logger("transform")


def hash_pii(value: str, salt: str = PII_HASH_SALT) -> str:
    """Compute a deterministic salted SHA-256 hex digest for safe downstream joins."""
    normalized = str(value).strip().lower()
    payload = f"{salt}:{normalized}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def mask_email(email: str) -> str:
    """Mask email local part while preserving first character and domain (e.g. j***@domain.com)."""
    cleaned = str(email).strip().lower()
    if "@" not in cleaned:
        return "***"
    local, domain = cleaned.split("@", 1)
    first_char = local[0] if local else "*"
    return f"{first_char}***@{domain}"


def mask_phone(phone: str) -> str:
    """Mask phone number preserving only the last 4 digits (e.g. XXXXXX7490)."""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) <= 4:
        return "X" * max(len(digits), 4)
    mask_len = max(len(digits) - 4, 2)
    return ("X" * mask_len) + digits[-4:]


def mask_ip(ip_address: str) -> str:
    """Mask the last two octets of an IPv4 address (e.g. 192.168.xxx.xxx)."""
    parts = str(ip_address).strip().split(".")
    if len(parts) != 4:
        return "xxx.xxx.xxx.xxx"
    return f"{parts[0]}.{parts[1]}.xxx.xxx"


def _to_utc_iso_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, format="mixed", errors="raise")
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def deduplicate_records(
    df: pd.DataFrame,
    primary_key: str,
    timestamp_col: str,
) -> tuple[pd.DataFrame, int]:
    """Deduplicate by primary key, retaining the latest valid record by timestamp."""
    if df.empty:
        return df.copy(), 0

    initial_count = len(df)
    work = df.copy()
    work["_sort_ts"] = pd.to_datetime(
        work[timestamp_col], utc=True, format="mixed", errors="coerce"
    )
    work = work.sort_values(by=["_sort_ts", primary_key], ascending=[True, True], kind="mergesort")
    work = work.drop_duplicates(subset=[primary_key], keep="last")
    work = work.drop(columns=["_sort_ts"]).reset_index(drop=True)

    removed = initial_count - len(work)
    return work, removed


def transform_customers(
    df: pd.DataFrame,
    salt: str = PII_HASH_SALT,
) -> tuple[pd.DataFrame, int]:
    """Deduplicate, standardize, impute, and mask PII for customer records."""
    deduped, dupes_removed = deduplicate_records(df, "customer_id", "signup_date")
    if deduped.empty:
        return pd.DataFrame(
            columns=[
                "customer_id",
                "full_name",
                "email_masked",
                "email_hash",
                "phone_masked",
                "phone_hash",
                "city",
                "signup_date",
                "kyc_status",
            ]
        ), dupes_removed

    out = pd.DataFrame()
    out["customer_id"] = deduped["customer_id"].astype(str).str.strip()
    out["full_name"] = (
        deduped["full_name"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.title()
    )

    raw_email = deduped["email"].astype(str).str.strip().str.lower()
    out["email_masked"] = raw_email.apply(mask_email)
    out["email_hash"] = raw_email.apply(lambda v: hash_pii(v, salt=salt))

    raw_phone_digits = deduped["phone"].astype(str).apply(lambda v: re.sub(r"\D", "", v))
    out["phone_masked"] = deduped["phone"].astype(str).apply(mask_phone)
    out["phone_hash"] = raw_phone_digits.apply(lambda v: hash_pii(v, salt=salt))

    city_series = deduped["city"].fillna("Unknown").astype(str).str.strip()
    city_series = city_series.replace({"": "Unknown", "nan": "Unknown", "None": "Unknown"})
    out["city"] = city_series.str.title()

    out["signup_date"] = _to_utc_iso_series(deduped["signup_date"])
    out["kyc_status"] = deduped["kyc_status"].astype(str).str.strip().str.lower()

    logger.info(
        "Transformed customers: output=%d, duplicates_removed=%d",
        len(out),
        dupes_removed,
    )
    return out, dupes_removed


def transform_orders(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Deduplicate, standardize, strip street PII, and normalize order items."""
    deduped, dupes_removed = deduplicate_records(df, "order_id", "order_timestamp")
    if deduped.empty:
        orders_empty = pd.DataFrame(
            columns=[
                "order_id",
                "customer_id",
                "order_timestamp",
                "amount",
                "currency",
                "payment_method",
                "shipping_country",
                "shipping_city",
                "shipping_postal_code",
                "item_count",
            ]
        )
        items_empty = pd.DataFrame(
            columns=[
                "order_item_id",
                "order_id",
                "line_number",
                "sku",
                "product_name",
                "category",
                "quantity",
                "unit_price",
                "line_total",
            ]
        )
        return orders_empty, items_empty, dupes_removed

    orders_out = pd.DataFrame()
    orders_out["order_id"] = deduped["order_id"].astype(str).str.strip()
    orders_out["customer_id"] = deduped["customer_id"].astype(str).str.strip()
    orders_out["order_timestamp"] = _to_utc_iso_series(deduped["order_timestamp"])
    orders_out["amount"] = pd.to_numeric(deduped["amount"]).round(2)
    orders_out["currency"] = deduped["currency"].astype(str).str.strip().str.upper()
    orders_out["payment_method"] = deduped["payment_method"].astype(str).str.strip().str.lower()

    # Strip raw street details from shipping_address while keeping geo metadata
    countries: list[str] = []
    cities: list[str] = []
    postals: list[str] = []
    item_counts: list[int] = []
    item_rows: list[dict[str, Any]] = []

    for row in deduped.itertuples(index=False):
        row_dict = row._asdict()
        oid = str(row_dict["order_id"]).strip()
        addr = row_dict.get("shipping_address") or {}
        countries.append(str(addr.get("country") or "Unknown").strip())
        cities.append(str(addr.get("city") or "Unknown").strip().title())
        postals.append(str(addr.get("postal_code") or "00000").strip())

        items = row_dict.get("items") or []
        item_counts.append(len(items))
        for idx, item in enumerate(items, start=1):
            qty = int(item["quantity"])
            unit_price = round(float(item["unit_price"]), 2)
            item_rows.append(
                {
                    "order_item_id": f"{oid}_{idx}",
                    "order_id": oid,
                    "line_number": idx,
                    "sku": str(item.get("sku", "UNKNOWN")).strip(),
                    "product_name": str(item.get("product_name", "Unknown Product")).strip(),
                    "category": str(item.get("category", "General")).strip(),
                    "quantity": qty,
                    "unit_price": unit_price,
                    "line_total": round(qty * unit_price, 2),
                }
            )

    orders_out["shipping_country"] = countries
    orders_out["shipping_city"] = cities
    orders_out["shipping_postal_code"] = postals
    orders_out["item_count"] = item_counts

    order_items_out = pd.DataFrame(item_rows)

    logger.info(
        "Transformed orders: orders=%d, order_items=%d, duplicates_removed=%d",
        len(orders_out),
        len(order_items_out),
        dupes_removed,
    )
    return orders_out, order_items_out, dupes_removed


def transform_activity_logs(
    df: pd.DataFrame,
    salt: str = PII_HASH_SALT,
) -> tuple[pd.DataFrame, int]:
    """Deduplicate, standardize, impute device_os, and mask/hash IP addresses."""
    deduped, dupes_removed = deduplicate_records(df, "event_id", "event_timestamp")
    if deduped.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "customer_id",
                "event_type",
                "ip_masked",
                "ip_hash",
                "device_os",
                "latency_ms",
                "event_timestamp",
            ]
        ), dupes_removed

    out = pd.DataFrame()
    out["event_id"] = deduped["event_id"].astype(str).str.strip()
    out["customer_id"] = deduped["customer_id"].astype(str).str.strip()
    out["event_type"] = deduped["event_type"].astype(str).str.strip().str.lower()

    raw_ip = deduped["ip_address"].astype(str).str.strip()
    out["ip_masked"] = raw_ip.apply(mask_ip)
    out["ip_hash"] = raw_ip.apply(lambda v: hash_pii(v, salt=salt))

    os_series = deduped["device_os"].fillna("Other").astype(str).str.strip()
    os_series = os_series.replace({"": "Other", "nan": "Other", "None": "Other"})
    out["device_os"] = os_series

    out["latency_ms"] = pd.to_numeric(deduped["latency_ms"]).astype(int)
    out["event_timestamp"] = _to_utc_iso_series(deduped["event_timestamp"])

    logger.info(
        "Transformed activity_logs: output=%d, duplicates_removed=%d",
        len(out),
        dupes_removed,
    )
    return out, dupes_removed
