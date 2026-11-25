"""Centralized configuration, directory paths, schema rules, and logging setup."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR: Path = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Directory Structure
DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
QUARANTINE_DIR: Path = DATA_DIR / "quarantine"
PROCESSED_DIR: Path = DATA_DIR / "processed"
LOGS_DIR: Path = BASE_DIR / "logs"
SQL_DIR: Path = BASE_DIR / "sql"

# Raw File Paths
CUSTOMERS_RAW_FILE: Path = RAW_DIR / "customers.csv"
ORDERS_RAW_FILE: Path = RAW_DIR / "orders.json"
ACTIVITY_LOGS_RAW_FILE: Path = RAW_DIR / "activity_logs.parquet"

# Database Settings
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "etl_warehouse")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "etl_password")

POSTGRES_DSN: str = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

MONGO_URI: str = os.getenv(
    "MONGO_URI",
    "mongodb://etl_user:etl_password@localhost:27018/etl_events?authSource=admin",
)
MONGO_DB: str = os.getenv("MONGO_DB", "etl_events")

# Security & PII Settings
PII_HASH_SALT: str = os.getenv(
    "PII_HASH_SALT", "prod-grade-sha256-salt-replace-in-prod"
)

# Execution Settings
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "2000"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
QUARANTINE_THRESHOLD_PCT: float = float(
    os.getenv("QUARANTINE_THRESHOLD_PCT", "20.0")
)

# Validation Regexes & Domain Rules
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
PHONE_REGEX = re.compile(r"^\+?[0-9\-\s\(\)]{7,20}$")
IPV4_REGEX = re.compile(
    r"^(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}"
    r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$"
)

ALLOWED_KYC_STATUSES: set[str] = {"verified", "pending", "rejected", "unverified"}
ALLOWED_CURRENCIES: set[str] = {"USD", "EUR", "GBP", "CAD", "AUD"}
ALLOWED_PAYMENT_METHODS: set[str] = {
    "credit_card",
    "debit_card",
    "bank_transfer",
    "wallet",
    "upi",
}
ALLOWED_EVENT_TYPES: set[str] = {
    "page_view",
    "login",
    "checkout_start",
    "payment_attempt",
    "payment_success",
    "logout",
    "profile_update",
}

MIN_ORDER_AMOUNT: float = 0.01
MAX_ORDER_AMOUNT: float = 1_000_000.00
MIN_LATENCY_MS: int = 0
MAX_LATENCY_MS: int = 120_000

SCHEMA_RULES: dict[str, dict[str, Any]] = {
    "customers": {
        "primary_key": "customer_id",
        "timestamp_col": "signup_date",
        "required_columns": [
            "customer_id",
            "full_name",
            "email",
            "phone",
            "city",
            "signup_date",
            "kyc_status",
        ],
        "non_null_columns": [
            "customer_id",
            "full_name",
            "email",
            "phone",
            "signup_date",
            "kyc_status",
        ],
    },
    "orders": {
        "primary_key": "order_id",
        "timestamp_col": "order_timestamp",
        "required_columns": [
            "order_id",
            "customer_id",
            "order_timestamp",
            "amount",
            "currency",
            "payment_method",
            "items",
            "shipping_address",
        ],
        "non_null_columns": [
            "order_id",
            "customer_id",
            "order_timestamp",
            "amount",
            "currency",
            "payment_method",
        ],
    },
    "activity_logs": {
        "primary_key": "event_id",
        "timestamp_col": "event_timestamp",
        "required_columns": [
            "event_id",
            "customer_id",
            "event_type",
            "ip_address",
            "device_os",
            "latency_ms",
            "event_timestamp",
        ],
        "non_null_columns": [
            "event_id",
            "customer_id",
            "event_type",
            "ip_address",
            "latency_ms",
            "event_timestamp",
        ],
    },
}


def ensure_directories() -> None:
    """Ensure all required data and log directories exist."""
    for directory in (RAW_DIR, QUARANTINE_DIR, PROCESSED_DIR, LOGS_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, log_file: Path | None = None) -> logging.Logger:
    """Configure and return a structured logger."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if log_file is not None:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    logger.propagate = False
    return logger
