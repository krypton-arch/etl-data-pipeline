"""Idempotent PostgreSQL batch loader using psycopg2 execute_values upserts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from config.settings import BATCH_SIZE, POSTGRES_DSN, SQL_DIR, get_logger

logger = get_logger("load_postgres")


def get_postgres_connection(dsn: str = POSTGRES_DSN) -> PgConnection:
    """Create a PostgreSQL connection."""
    return psycopg2.connect(dsn)


def ensure_postgres_schema(
    conn: PgConnection,
    sql_path: Path = SQL_DIR / "init_postgres.sql",
) -> None:
    """Execute idempotent DDL script to ensure 3NF tables and indexes exist."""
    if not sql_path.exists():
        raise FileNotFoundError(f"PostgreSQL DDL file not found: {sql_path}")
    ddl = sql_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Verified PostgreSQL schema from %s", sql_path.name)


def _batched_execute_values(
    conn: PgConnection,
    sql: str,
    rows: list[tuple[Any, ...]],
    batch_size: int = BATCH_SIZE,
) -> int:
    if not rows:
        return 0
    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            chunk = rows[start : start + batch_size]
            execute_values(cur, sql, chunk, page_size=batch_size)
            total += len(chunk)
    conn.commit()
    return total


def upsert_customers(
    conn: PgConnection,
    customers_df: pd.DataFrame,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Idempotently upsert transformed customer records into dim_customers."""
    if customers_df.empty:
        return 0

    sql = """
        INSERT INTO dim_customers (
            customer_id, full_name, email_masked, email_hash,
            phone_masked, phone_hash, city, signup_date, kyc_status, updated_at
        )
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            email_masked = EXCLUDED.email_masked,
            email_hash = EXCLUDED.email_hash,
            phone_masked = EXCLUDED.phone_masked,
            phone_hash = EXCLUDED.phone_hash,
            city = EXCLUDED.city,
            signup_date = EXCLUDED.signup_date,
            kyc_status = EXCLUDED.kyc_status,
            updated_at = NOW();
    """
    rows = [
        (
            r.customer_id,
            r.full_name,
            r.email_masked,
            r.email_hash,
            r.phone_masked,
            r.phone_hash,
            r.city,
            r.signup_date,
            r.kyc_status,
        )
        for r in customers_df.itertuples(index=False)
    ]
    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            chunk = rows[start : start + batch_size]
            execute_values(cur, sql, chunk, template=template, page_size=batch_size)
            total += len(chunk)
    conn.commit()
    logger.info("Upserted %d rows into dim_customers", total)
    return total


def upsert_orders(
    conn: PgConnection,
    orders_df: pd.DataFrame,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Idempotently upsert transformed order records into fact_orders."""
    if orders_df.empty:
        return 0

    sql = """
        INSERT INTO fact_orders (
            order_id, customer_id, order_timestamp, amount, currency,
            payment_method, shipping_country, shipping_city,
            shipping_postal_code, item_count, updated_at
        )
        VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_timestamp = EXCLUDED.order_timestamp,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            payment_method = EXCLUDED.payment_method,
            shipping_country = EXCLUDED.shipping_country,
            shipping_city = EXCLUDED.shipping_city,
            shipping_postal_code = EXCLUDED.shipping_postal_code,
            item_count = EXCLUDED.item_count,
            updated_at = NOW();
    """
    rows = [
        (
            r.order_id,
            r.customer_id,
            r.order_timestamp,
            float(r.amount),
            r.currency,
            r.payment_method,
            r.shipping_country,
            r.shipping_city,
            r.shipping_postal_code,
            int(r.item_count),
        )
        for r in orders_df.itertuples(index=False)
    ]
    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            chunk = rows[start : start + batch_size]
            execute_values(cur, sql, chunk, template=template, page_size=batch_size)
            total += len(chunk)
    conn.commit()
    logger.info("Upserted %d rows into fact_orders", total)
    return total


def upsert_order_items(
    conn: PgConnection,
    order_items_df: pd.DataFrame,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Idempotently upsert normalized order line items into fact_order_items."""
    if order_items_df.empty:
        return 0

    sql = """
        INSERT INTO fact_order_items (
            order_item_id, order_id, line_number, sku,
            product_name, category, quantity, unit_price, line_total
        )
        VALUES %s
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id = EXCLUDED.order_id,
            line_number = EXCLUDED.line_number,
            sku = EXCLUDED.sku,
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            line_total = EXCLUDED.line_total;
    """
    rows = [
        (
            r.order_item_id,
            r.order_id,
            int(r.line_number),
            r.sku,
            r.product_name,
            r.category,
            int(r.quantity),
            float(r.unit_price),
            float(r.line_total),
        )
        for r in order_items_df.itertuples(index=False)
    ]
    total = _batched_execute_values(conn, sql, rows, batch_size=batch_size)
    logger.info("Upserted %d rows into fact_order_items", total)
    return total


def record_batch_run(conn: PgConnection, batch_meta: dict[str, Any]) -> None:
    """Persist ETL batch execution metrics into etl_batch_runs."""
    sql = """
        INSERT INTO etl_batch_runs (
            batch_id, started_at, completed_at, duration_seconds, status,
            total_records_read, valid_records, quarantined_records,
            duplicates_removed, postgres_rows_loaded, mongo_docs_upserted,
            dq_report_path
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id) DO UPDATE SET
            completed_at = EXCLUDED.completed_at,
            duration_seconds = EXCLUDED.duration_seconds,
            status = EXCLUDED.status,
            total_records_read = EXCLUDED.total_records_read,
            valid_records = EXCLUDED.valid_records,
            quarantined_records = EXCLUDED.quarantined_records,
            duplicates_removed = EXCLUDED.duplicates_removed,
            postgres_rows_loaded = EXCLUDED.postgres_rows_loaded,
            mongo_docs_upserted = EXCLUDED.mongo_docs_upserted,
            dq_report_path = EXCLUDED.dq_report_path;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                batch_meta["batch_id"],
                batch_meta["started_at"],
                batch_meta["completed_at"],
                batch_meta["duration_seconds"],
                batch_meta["status"],
                batch_meta["total_records_read"],
                batch_meta["valid_records"],
                batch_meta["quarantined_records"],
                batch_meta["duplicates_removed"],
                batch_meta["postgres_rows_loaded"],
                batch_meta["mongo_docs_upserted"],
                batch_meta.get("dq_report_path"),
            ),
        )
    conn.commit()
    logger.info("Recorded ETL batch run '%s' in etl_batch_runs", batch_meta["batch_id"])


def get_postgres_table_counts(conn: PgConnection) -> dict[str, int]:
    """Return row counts for all warehouse tables."""
    tables = ["dim_customers", "fact_orders", "fact_order_items", "etl_batch_runs"]
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            counts[table] = int(cur.fetchone()[0])
    return counts


def load_to_postgres(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    dsn: str = POSTGRES_DSN,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    """Connect, ensure schema, and upsert customers, orders, and order items."""
    conn = get_postgres_connection(dsn)
    try:
        ensure_postgres_schema(conn)
        c_count = upsert_customers(conn, customers_df, batch_size=batch_size)
        o_count = upsert_orders(conn, orders_df, batch_size=batch_size)
        oi_count = upsert_order_items(conn, order_items_df, batch_size=batch_size)
        return {
            "dim_customers": c_count,
            "fact_orders": o_count,
            "fact_order_items": oi_count,
            "total_upserted": c_count + o_count + oi_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
