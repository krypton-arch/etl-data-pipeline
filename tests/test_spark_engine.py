"""Unit tests for the PySpark ETL engine and interactive engine selection."""

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from src.pipeline import resolve_engine_choice

pyspark = pytest.importorskip("pyspark")


def _has_java() -> bool:
    return shutil.which("java") is not None


def test_resolve_engine_choice_cli_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    assert resolve_engine_choice("pandas") == "pandas"
    assert resolve_engine_choice("pyspark") == "pyspark"

    monkeypatch.setenv("ETL_ENGINE", "pyspark")
    assert resolve_engine_choice(None) == "pyspark"

    monkeypatch.delenv("ETL_ENGINE", raising=False)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert resolve_engine_choice(None) == "pandas"


def test_resolve_engine_choice_interactive_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_ENGINE", raising=False)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "2")
    assert resolve_engine_choice(None) == "pyspark"

    monkeypatch.setattr("builtins.input", lambda _prompt: "1")
    assert resolve_engine_choice(None) == "pandas"


@pytest.mark.skipif(not _has_java(), reason="Java runtime required for local PySpark tests")
def test_spark_etl_stages_end_to_end(tmp_path: Path) -> None:
    from src.spark_engine import create_spark_session, run_spark_etl_stages

    raw_dir = tmp_path / "raw"
    q_dir = tmp_path / "quarantine"
    logs_dir = tmp_path / "logs"
    raw_dir.mkdir()

    cust_csv = raw_dir / "customers.csv"
    pd.DataFrame(
        [
            {
                "customer_id": "CUST-001",
                "full_name": "  alice   SMITH ",
                "email": "Alice@Example.com",
                "phone": "5551237490",
                "city": None,
                "signup_date": "2025-01-15 10:00:00",
                "kyc_status": "PENDING",
            },
            {
                "customer_id": "CUST-001",
                "full_name": "  alice   SMITH ",
                "email": "Alice@Example.com",
                "phone": "5551237490",
                "city": "austin",
                "signup_date": "2025-02-01T12:00:00+00:00",
                "kyc_status": "VERIFIED",
            },
            {
                "customer_id": None,
                "full_name": "Bad Row",
                "email": "bad-email",
                "phone": "123",
                "city": "NYC",
                "signup_date": "not-a-date",
                "kyc_status": "bad_status",
            },
        ]
    ).to_csv(cust_csv, index=False)

    orders_json = raw_dir / "orders.json"
    orders_payload = [
        {
            "order_id": "ORD-1",
            "customer_id": "CUST-001",
            "order_timestamp": "2025-03-10T08:30:00+00:00",
            "amount": 150.25,
            "currency": "usd",
            "payment_method": "CREDIT_CARD",
            "items": [
                {
                    "sku": "SKU-1",
                    "product_name": "Headphones",
                    "category": "Electronics",
                    "quantity": 1,
                    "unit_price": 150.25,
                }
            ],
            "shipping_address": {
                "street": "123 Secret St",
                "city": "austin",
                "state": "TX",
                "postal_code": "78701",
                "country": "USA",
            },
        },
        {
            "order_id": "ORD-BAD",
            "customer_id": "CUST-ORPHAN",
            "order_timestamp": "bad-ts",
            "amount": -10.0,
            "currency": "ZZZ",
            "payment_method": "credit_card",
            "items": [],
            "shipping_address": None,
        },
    ]
    orders_json.write_text(json.dumps(orders_payload), encoding="utf-8")

    activity_parquet = raw_dir / "activity_logs.parquet"
    pd.DataFrame(
        [
            {
                "event_id": "EVT-1",
                "customer_id": "CUST-001",
                "event_type": "LOGIN",
                "ip_address": "192.168.10.25",
                "device_os": None,
                "latency_ms": 45,
                "event_timestamp": "2025-04-01T10:00:00+00:00",
            },
            {
                "event_id": "EVT-BAD",
                "customer_id": "CUST-001",
                "event_type": "login",
                "ip_address": "999.999.1.1",
                "device_os": "Linux",
                "latency_ms": -5,
                "event_timestamp": "2025-04-01T10:05:00+00:00",
            },
        ]
    ).to_parquet(activity_parquet, index=False)

    spark = create_spark_session(app_name="TestSparkETL")
    try:
        out = run_spark_etl_stages(
            spark,
            batch_id="batch_spark_test",
            run_ts="20261125_230000",
            customers_path=cust_csv,
            orders_path=orders_json,
            activity_path=activity_parquet,
            quarantine_dir=q_dir,
            logs_dir=logs_dir,
            salt="test-salt",
        )
    finally:
        spark.stop()

    cust_df = out["customers_clean"]
    assert len(cust_df) == 1
    assert cust_df.iloc[0]["kyc_status"] == "verified"
    assert cust_df.iloc[0]["email_masked"] == "a***@example.com"
    assert cust_df.iloc[0]["phone_masked"] == "XXXXXX7490"
    assert "email" not in cust_df.columns

    orders_df = out["orders_clean"]
    assert len(orders_df) == 1
    assert orders_df.iloc[0]["currency"] == "USD"
    assert "shipping_street" not in orders_df.columns

    items_df = out["order_items_clean"]
    assert len(items_df) == 1
    assert items_df.iloc[0]["order_item_id"] == "ORD-1_1"

    act_df = out["activity_clean"]
    assert len(act_df) == 1
    assert act_df.iloc[0]["ip_masked"] == "192.168.xxx.xxx"
    assert act_df.iloc[0]["device_os"] == "Other"

    dq = out["dq_report"]
    assert dq["summary"]["total_rows_read"] == 7
    assert dq["summary"]["total_valid_rows"] == 4
    assert dq["summary"]["total_quarantined_rows"] == 3
    assert out["dq_report_path"].exists()
