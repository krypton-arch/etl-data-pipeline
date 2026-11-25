"""Unit tests for deduplication, null imputation, standardization, and PII masking."""

import pandas as pd

from src.transform import (
    hash_pii,
    mask_email,
    mask_ip,
    mask_phone,
    transform_activity_logs,
    transform_customers,
    transform_orders,
)


def test_pii_masking_and_salted_hashing() -> None:
    assert mask_email("John.Doe@Domain.com") == "j***@domain.com"
    assert mask_phone("+1-555-123-7490") == "XXXXXXX7490"
    assert mask_phone("5551237490") == "XXXXXX7490"
    assert mask_ip("192.168.10.25") == "192.168.xxx.xxx"

    h1 = hash_pii("Alice@Example.com", salt="unit-salt")
    h2 = hash_pii("  alice@example.com ", salt="unit-salt")
    h3 = hash_pii("alice@example.com", salt="other-salt")
    assert len(h1) == 64
    assert h1 == h2
    assert h1 != h3


def test_transform_customers_deduplication_imputation_and_pii_removal() -> None:
    df = pd.DataFrame(
        [
            {
                "customer_id": "CUST-001",
                "full_name": "  alice   SMITH ",
                "email": "Alice.Smith@Gmail.com",
                "phone": "5551237490",
                "city": None,
                "signup_date": "2025-01-01 10:00:00",
                "kyc_status": "PENDING",
            },
            {
                "customer_id": "CUST-001",
                "full_name": "  alice   SMITH ",
                "email": "Alice.Smith@Gmail.com",
                "phone": "5551237490",
                "city": "new york",
                "signup_date": "2025-02-01T12:00:00+00:00",
                "kyc_status": "VERIFIED",
            },
            {
                "customer_id": "CUST-002",
                "full_name": "bob jones",
                "email": "bob@corp.net",
                "phone": "5559998888",
                "city": "",
                "signup_date": "2025-01-10T08:00:00+00:00",
                "kyc_status": "verified",
            },
        ]
    )

    clean_df, dupes_removed = transform_customers(df, salt="test-salt")

    assert dupes_removed == 1
    assert len(clean_df) == 2

    # Raw PII columns must be stripped
    assert "email" not in clean_df.columns
    assert "phone" not in clean_df.columns

    row1 = clean_df.loc[clean_df["customer_id"] == "CUST-001"].iloc[0]
    assert row1["full_name"] == "Alice Smith"
    assert row1["kyc_status"] == "verified"  # latest record kept
    assert row1["city"] == "New York"
    assert row1["email_masked"] == "a***@gmail.com"
    assert row1["phone_masked"] == "XXXXXX7490"
    assert row1["signup_date"] == "2025-02-01T12:00:00+00:00"

    row2 = clean_df.loc[clean_df["customer_id"] == "CUST-002"].iloc[0]
    assert row2["city"] == "Unknown"


def test_transform_orders_strips_street_and_normalizes_items() -> None:
    df = pd.DataFrame(
        [
            {
                "order_id": "ORD-100",
                "customer_id": "CUST-001",
                "order_timestamp": "2025-05-01T10:00:00+00:00",
                "amount": "249.50",
                "currency": "eur",
                "payment_method": "CREDIT_CARD",
                "items": [
                    {
                        "sku": "SKU-1",
                        "product_name": "Keyboard",
                        "category": "Electronics",
                        "quantity": 2,
                        "unit_price": 100.0,
                    },
                    {
                        "sku": "SKU-2",
                        "product_name": "Mouse",
                        "category": "Accessories",
                        "quantity": 1,
                        "unit_price": 49.5,
                    },
                ],
                "shipping_address": {
                    "street": "742 Evergreen Terrace",
                    "city": "berlin",
                    "postal_code": "10115",
                    "country": "Germany",
                },
            },
            # Exact/key duplicate with older timestamp
            {
                "order_id": "ORD-100",
                "customer_id": "CUST-001",
                "order_timestamp": "2025-04-01T10:00:00+00:00",
                "amount": "200.00",
                "currency": "eur",
                "payment_method": "credit_card",
                "items": [
                    {
                        "sku": "SKU-1",
                        "product_name": "Keyboard",
                        "category": "Electronics",
                        "quantity": 2,
                        "unit_price": 100.0,
                    }
                ],
                "shipping_address": {
                    "street": "742 Evergreen Terrace",
                    "city": "berlin",
                    "postal_code": "10115",
                    "country": "Germany",
                },
            },
        ]
    )

    orders_df, items_df, dupes_removed = transform_orders(df)

    assert dupes_removed == 1
    assert len(orders_df) == 1
    assert "shipping_address" not in orders_df.columns
    assert "shipping_street" not in orders_df.columns
    assert orders_df.iloc[0]["currency"] == "EUR"
    assert orders_df.iloc[0]["payment_method"] == "credit_card"
    assert orders_df.iloc[0]["shipping_city"] == "Berlin"
    assert orders_df.iloc[0]["amount"] == 249.50
    assert orders_df.iloc[0]["item_count"] == 2

    assert len(items_df) == 2
    assert list(items_df["order_item_id"]) == ["ORD-100_1", "ORD-100_2"]
    assert items_df.iloc[0]["line_total"] == 200.0


def test_transform_activity_logs_imputes_os_and_masks_ip() -> None:
    df = pd.DataFrame(
        [
            {
                "event_id": "EVT-1",
                "customer_id": "CUST-1",
                "event_type": "CHECKOUT_START",
                "ip_address": "10.20.30.40",
                "device_os": None,
                "latency_ms": 120.0,
                "event_timestamp": "2025-06-01T15:00:00Z",
            }
        ]
    )

    clean_df, dupes_removed = transform_activity_logs(df, salt="test-salt")
    assert dupes_removed == 0
    assert "ip_address" not in clean_df.columns
    assert clean_df.iloc[0]["ip_masked"] == "10.20.xxx.xxx"
    assert clean_df.iloc[0]["device_os"] == "Other"
    assert clean_df.iloc[0]["event_type"] == "checkout_start"
    assert clean_df.iloc[0]["latency_ms"] == 120
