"""Unit tests for schema enforcement, range/regex validation, quarantine routing, and DQ reports."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.validate import (
    generate_dq_report,
    validate_activity_logs,
    validate_customers,
    validate_orders,
)


def test_validate_customers_routes_invalid_records_to_quarantine(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "customer_id": "CUST-000001",
                "full_name": "Alice Johnson",
                "email": "alice@example.com",
                "phone": "+1-555-234-7490",
                "city": "Austin",
                "signup_date": "2025-01-15T10:00:00+00:00",
                "kyc_status": "verified",
            },
            {
                "customer_id": "CUST-000002",
                "full_name": "Mixed Date User",
                "email": "mixed@example.com",
                "phone": "+1-555-234-7491",
                "city": "Seattle",
                "signup_date": "2025-01-20 14:30:00",
                "kyc_status": "verified",
            },
            {
                "customer_id": None,
                "full_name": "Missing ID",
                "email": "bad-email",
                "phone": "123",
                "city": "Boston",
                "signup_date": "not-a-date",
                "kyc_status": "invalid_tier",
            },
            {
                "customer_id": "CUST-000003",
                "full_name": "Bad Email User",
                "email": "not_an_email.com",
                "phone": "+1-555-111-2222",
                "city": None,
                "signup_date": "2025-02-01T12:00:00+00:00",
                "kyc_status": "pending",
            },
        ]
    )

    result = validate_customers(df, timestamp="20261125_000000", quarantine_dir=tmp_path)

    assert len(result.valid_df) == 2
    assert result.valid_df.iloc[0]["customer_id"] == "CUST-000001"
    assert result.valid_df.iloc[1]["customer_id"] == "CUST-000002"
    assert len(result.quarantined_df) == 2
    assert result.quarantine_file is not None
    assert result.quarantine_file.exists()

    quarantined_records = json.loads(result.quarantine_file.read_text(encoding="utf-8"))
    assert len(quarantined_records) == 2
    assert "null_customer_id" in quarantined_records[0]["rejection_reason"]
    assert "invalid_email_format" in quarantined_records[0]["rejection_reason"]
    assert "invalid_email_format" in quarantined_records[1]["rejection_reason"]


def test_validate_customers_raises_on_missing_schema_columns(tmp_path: Path) -> None:
    bad_df = pd.DataFrame([{"customer_id": "CUST-1", "full_name": "Only Two Cols"}])
    with pytest.raises(ValueError, match="missing required columns"):
        validate_customers(bad_df, timestamp="20261125_000000", quarantine_dir=tmp_path)


def test_validate_orders_enforces_amount_timestamp_currency_and_fk(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-1",
                "order_timestamp": "2025-03-10T08:30:00+00:00",
                "amount": 150.25,
                "currency": "usd",
                "payment_method": "credit_card",
                "items": [{"sku": "SKU-1", "quantity": 1, "unit_price": 150.25}],
                "shipping_address": {"street": "1 Main", "city": "NYC", "country": "USA"},
            },
            {
                "order_id": "ORD-2",
                "customer_id": "CUST-1",
                "order_timestamp": "bad-timestamp",
                "amount": -42.0,
                "currency": "ZZZ",
                "payment_method": "credit_card",
                "items": [],
                "shipping_address": {"street": "1 Main", "city": "NYC", "country": "USA"},
            },
            {
                "order_id": "ORD-3",
                "customer_id": "CUST-ORPHAN",
                "order_timestamp": "2025-03-10T09:00:00+00:00",
                "amount": 99.0,
                "currency": "USD",
                "payment_method": "upi",
                "items": [{"sku": "SKU-1", "quantity": 1, "unit_price": 99.0}],
                "shipping_address": {"street": "1 Main", "city": "NYC", "country": "USA"},
            },
        ]
    )

    result = validate_orders(
        df,
        timestamp="20261125_000000",
        quarantine_dir=tmp_path,
        valid_customer_ids={"CUST-1"},
    )

    assert len(result.valid_df) == 1
    assert result.valid_df.iloc[0]["order_id"] == "ORD-1"
    assert len(result.quarantined_df) == 2
    assert "amount_out_of_range" in result.quarantined_df.iloc[0]["rejection_reason"]
    assert "invalid_order_timestamp" in result.quarantined_df.iloc[0]["rejection_reason"]
    assert "unsupported_currency" in result.quarantined_df.iloc[0]["rejection_reason"]
    assert "invalid_or_empty_items" in result.quarantined_df.iloc[0]["rejection_reason"]
    assert "orphan_customer_id" in result.quarantined_df.iloc[1]["rejection_reason"]


def test_validate_activity_logs_and_dq_report(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "event_id": "EVT-1",
                "customer_id": "CUST-1",
                "event_type": "login",
                "ip_address": "192.168.10.25",
                "device_os": "macOS 15",
                "latency_ms": 45,
                "event_timestamp": "2025-04-01T10:00:00+00:00",
            },
            {
                "event_id": "EVT-2",
                "customer_id": "CUST-1",
                "event_type": "login",
                "ip_address": "999.999.1.1",
                "device_os": "Windows 11",
                "latency_ms": -10,
                "event_timestamp": "2025-04-01T10:05:00+00:00",
            },
        ]
    )

    q_dir = tmp_path / "quarantine"
    logs_dir = tmp_path / "logs"

    res = validate_activity_logs(df, timestamp="20261125_000000", quarantine_dir=q_dir)
    assert len(res.valid_df) == 1
    assert len(res.quarantined_df) == 1
    assert "invalid_ip_address" in res.quarantined_df.iloc[0]["rejection_reason"]
    assert "latency_out_of_range" in res.quarantined_df.iloc[0]["rejection_reason"]

    report, report_path = generate_dq_report(
        {"activity_logs": res},
        batch_id="batch_test",
        timestamp="20261125_000000",
        logs_dir=logs_dir,
    )
    assert report_path.exists()
    assert report["summary"]["total_rows_read"] == 2
    assert report["summary"]["total_valid_rows"] == 1
    assert report["summary"]["total_quarantined_rows"] == 1
