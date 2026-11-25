"""Synthetic multi-source data generator producing realistic messy datasets."""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import (
    ACTIVITY_LOGS_RAW_FILE,
    CUSTOMERS_RAW_FILE,
    ORDERS_RAW_FILE,
    ensure_directories,
    get_logger,
)

logger = get_logger("generate_sample_data")

FIRST_NAMES = [
    "Aarav", "Olivia", "Liam", "Emma", "Noah", "Sophia", "Ethan", "Maya",
    "Lucas", "Isabella", "Mason", "Ava", "Logan", "Mia", "Elijah", "Priya",
    "Daniel", "Chloe", "Henry", "Zara", "Nikhil", "Hannah", "Leo", "Elena",
]
LAST_NAMES = [
    "Sharma", "Patel", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Kim", "Singh",
]
DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "proton.me", "fintech.io", "corp.net"]
CITIES = [
    ("New York", "NY", "10001", "USA"),
    ("San Francisco", "CA", "94105", "USA"),
    ("Austin", "TX", "78701", "USA"),
    ("London", "LDN", "EC1A1BB", "UK"),
    ("Toronto", "ON", "M5V2T6", "Canada"),
    ("Berlin", "BE", "10115", "Germany"),
    ("Sydney", "NSW", "2000", "Australia"),
    ("Bengaluru", "KA", "560001", "India"),
]
STREETS = [
    "Market St", "Broadway", "Oak Ave", "Pine Rd", "Elm St",
    "MG Road", "King St", "Sunset Blvd", "5th Ave", "High St",
]
PRODUCTS = [
    ("SKU-101", "Wireless Noise-Canceling Headphones", "Electronics", 199.99),
    ("SKU-102", "Mechanical Keyboard RGB", "Electronics", 129.50),
    ("SKU-103", "Ergonomic Laptop Stand", "Accessories", 49.00),
    ("SKU-104", "USB-C Multiport Hub", "Accessories", 39.95),
    ("SKU-105", "4K IPS Monitor 27-inch", "Electronics", 379.00),
    ("SKU-106", "Smart Fitness Watch", "Wearables", 249.00),
    ("SKU-107", "Hardware Security Key", "Security", 55.00),
    ("SKU-108", "Stainless Insulated Tumbler", "Lifestyle", 28.50),
]
KYC_STATUSES = ["verified", "pending", "rejected", "unverified"]
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "wallet", "upi"]
EVENT_TYPES = [
    "page_view", "login", "checkout_start", "payment_attempt",
    "payment_success", "logout", "profile_update",
]
DEVICE_OS_LIST = ["Windows 11", "macOS 15", "iOS 18", "Android 15", "Ubuntu Linux"]


def _random_timestamp(rng: random.Random, start: datetime, days: int) -> datetime:
    offset_seconds = rng.randint(0, days * 86400)
    return start + timedelta(seconds=offset_seconds)


def generate_customers(
    n_rows: int = 10_000,
    seed: int = 42,
    output_path: Path = CUSTOMERS_RAW_FILE,
) -> tuple[pd.DataFrame, list[str]]:
    """Generate ~10,000 customer rows with intentional quality defects (~6%)."""
    rng = random.Random(seed)
    base_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows: list[dict[str, Any]] = []
    valid_customer_ids: list[str] = []

    n_unique = int(n_rows * 0.96)
    for i in range(1, n_unique + 1):
        cid = f"CUST-{i:06d}"
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        full_name = f"  {first.lower()} {last.upper()}  " if i % 7 == 0 else f"{first} {last}"
        domain = rng.choice(DOMAINS)
        email = f"{first.lower()}.{last.lower()}{i}@{domain}"
        phone = f"+1-{rng.randint(200, 999)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"
        city = rng.choice(CITIES)[0]
        signup_dt = _random_timestamp(rng, base_dt, 600)
        signup_str = signup_dt.strftime("%Y-%m-%d %H:%M:%S") if i % 5 == 0 else signup_dt.isoformat()
        kyc_status = rng.choices(KYC_STATUSES, weights=[0.7, 0.15, 0.05, 0.10])[0]

        is_corrupt = False
        defect_roll = rng.random()
        if defect_roll < 0.012:
            cid = None  # null PK
            is_corrupt = True
        elif defect_roll < 0.025:
            email = rng.choice(["invalid-email", "missing_at.com", None, "user@.com"])
            is_corrupt = True
        elif defect_roll < 0.036:
            phone = rng.choice(["123", "abc-def", None])
            is_corrupt = True
        elif defect_roll < 0.046:
            signup_str = rng.choice(["not-a-date", "2024/99/99", ""])
            is_corrupt = True
        elif defect_roll < 0.054:
            kyc_status = "unknown_tier"
            is_corrupt = True
        elif defect_roll < 0.08:
            city = None  # non-critical null; should be imputed to "Unknown"

        if not is_corrupt and cid is not None:
            valid_customer_ids.append(cid)

        rows.append(
            {
                "customer_id": cid,
                "full_name": full_name,
                "email": email,
                "phone": phone,
                "city": city,
                "signup_date": signup_str,
                "kyc_status": kyc_status,
            }
        )

    # Inject exact duplicates and key-level duplicates with newer signup timestamps
    n_dupes = n_rows - len(rows)
    for j in range(n_dupes):
        source_row = dict(rows[rng.randrange(0, len(rows))])
        if j % 2 == 0 and source_row.get("customer_id") is not None:
            # Newer record for same customer_id
            source_row["signup_date"] = (base_dt + timedelta(days=650 + j)).isoformat()
            source_row["kyc_status"] = "verified"
        rows.append(source_row)

    rng.shuffle(rows)
    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Generated %d customer rows -> %s", len(df), output_path)
    return df, valid_customer_ids


def generate_orders(
    valid_customer_ids: list[str],
    n_rows: int = 25_000,
    seed: int = 42,
    output_path: Path = ORDERS_RAW_FILE,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Generate ~25,000 nested JSON order records with ~6% quality defects."""
    rng = random.Random(seed + 1)
    base_dt = datetime(2024, 6, 1, tzinfo=timezone.utc)
    records: list[dict[str, Any]] = []
    valid_order_ids: list[str] = []

    n_unique = int(n_rows * 0.96)
    for i in range(1, n_unique + 1):
        oid = f"ORD-{i:07d}"
        cid = rng.choice(valid_customer_ids)
        order_dt = _random_timestamp(rng, base_dt, 450)
        order_ts = order_dt.isoformat()

        n_items = rng.randint(1, 3)
        chosen_products = rng.sample(PRODUCTS, k=n_items)
        items: list[dict[str, Any]] = []
        calc_total = 0.0
        for sku, name, cat, price in chosen_products:
            qty = rng.randint(1, 3)
            calc_total += price * qty
            items.append(
                {
                    "sku": sku,
                    "product_name": name,
                    "category": cat,
                    "quantity": qty,
                    "unit_price": round(price, 2),
                }
            )

        amount: Any = round(calc_total, 2)
        currency = rng.choices(CURRENCIES, weights=[0.6, 0.15, 0.1, 0.08, 0.07])[0]
        if i % 9 == 0:
            currency = currency.lower()  # standardization test case
        payment_method = rng.choice(PAYMENT_METHODS)

        city_name, state, postal, country = rng.choice(CITIES)
        street = f"{rng.randint(10, 9999)} {rng.choice(STREETS)}"
        shipping_address = {
            "street": street,
            "city": city_name,
            "state": state,
            "postal_code": postal,
            "country": country,
        }

        is_corrupt = False
        defect_roll = rng.random()
        if defect_roll < 0.012:
            oid = None
            is_corrupt = True
        elif defect_roll < 0.025:
            amount = round(-abs(calc_total), 2)  # negative amount
            is_corrupt = True
        elif defect_roll < 0.035:
            amount = "INVALID_AMT"  # non-numeric amount
            is_corrupt = True
        elif defect_roll < 0.047:
            order_ts = "2024-13-45T99:00:00"  # malformed timestamp
            is_corrupt = True
        elif defect_roll < 0.055:
            currency = "XYZ"  # unsupported currency
            is_corrupt = True
        elif defect_roll < 0.062:
            cid = None  # null foreign key
            is_corrupt = True

        if not is_corrupt and oid is not None:
            valid_order_ids.append(oid)

        records.append(
            {
                "order_id": oid,
                "customer_id": cid,
                "order_timestamp": order_ts,
                "amount": amount,
                "currency": currency,
                "payment_method": payment_method,
                "items": items,
                "shipping_address": shipping_address,
            }
        )

    # Inject duplicate orders
    n_dupes = n_rows - len(records)
    for j in range(n_dupes):
        dup = dict(records[rng.randrange(0, len(records))])
        if j % 2 == 0 and dup.get("order_id") is not None:
            dup["order_timestamp"] = (base_dt + timedelta(days=500 + j)).isoformat()
        records.append(dup)

    rng.shuffle(records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)
    logger.info("Generated %d nested order records -> %s", len(records), output_path)
    return records, valid_order_ids


def generate_activity_logs(
    valid_customer_ids: list[str],
    n_rows: int = 20_000,
    seed: int = 42,
    output_path: Path = ACTIVITY_LOGS_RAW_FILE,
) -> pd.DataFrame:
    """Generate ~20,000 Parquet activity log rows with ~6% quality defects."""
    rng = random.Random(seed + 2)
    base_dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows: list[dict[str, Any]] = []

    n_unique = int(n_rows * 0.96)
    for i in range(1, n_unique + 1):
        eid = f"EVT-{i:07d}"
        cid = rng.choice(valid_customer_ids)
        event_type = rng.choice(EVENT_TYPES)
        ip = f"{rng.randint(11, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
        device_os: str | None = rng.choice(DEVICE_OS_LIST)
        latency_ms: Any = rng.randint(12, 1850)
        event_ts = _random_timestamp(rng, base_dt, 300).isoformat()

        defect_roll = rng.random()
        if defect_roll < 0.012:
            eid = None
        elif defect_roll < 0.025:
            ip = rng.choice(["999.999.1.1", "not_an_ip", None])
        elif defect_roll < 0.038:
            latency_ms = -rng.randint(1, 500)
        elif defect_roll < 0.050:
            event_ts = "invalid-timestamp"
        elif defect_roll < 0.058:
            event_type = "unknown_event"
        elif defect_roll < 0.085:
            device_os = None  # non-critical null; imputed to "Other"

        rows.append(
            {
                "event_id": eid,
                "customer_id": cid,
                "event_type": event_type,
                "ip_address": ip,
                "device_os": device_os,
                "latency_ms": latency_ms,
                "event_timestamp": event_ts,
            }
        )

    n_dupes = n_rows - len(rows)
    for j in range(n_dupes):
        dup = dict(rows[rng.randrange(0, len(rows))])
        if j % 2 == 0 and dup.get("event_id") is not None:
            dup["event_timestamp"] = (base_dt + timedelta(days=320 + j)).isoformat()
        rows.append(dup)

    rng.shuffle(rows)
    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info("Generated %d activity log rows -> %s", len(df), output_path)
    return df


def generate_all(
    customers_count: int = 10_000,
    orders_count: int = 25_000,
    activity_count: int = 20_000,
    seed: int = 42,
) -> dict[str, int]:
    """Generate all three multi-source datasets in data/raw/."""
    ensure_directories()
    customers_df, valid_cids = generate_customers(customers_count, seed=seed)
    orders_list, _ = generate_orders(valid_cids, orders_count, seed=seed)
    activity_df = generate_activity_logs(valid_cids, activity_count, seed=seed)

    summary = {
        "customers": len(customers_df),
        "orders": len(orders_list),
        "activity_logs": len(activity_df),
        "total": len(customers_df) + len(orders_list) + len(activity_df),
    }
    logger.info("Total synthetic records generated: %d", summary["total"])
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic messy multi-source ETL data.")
    parser.add_argument("--customers", type=int, default=10_000, help="Number of customer rows")
    parser.add_argument("--orders", type=int, default=25_000, help="Number of order records")
    parser.add_argument("--activity", type=int, default=20_000, help="Number of activity log rows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    generate_all(
        customers_count=args.customers,
        orders_count=args.orders,
        activity_count=args.activity,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
