"""Main CLI entrypoint orchestrating Extract -> Validate -> Transform -> Load."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any

from config.settings import (
    ACTIVITY_LOGS_RAW_FILE,
    BATCH_SIZE,
    CUSTOMERS_RAW_FILE,
    LOGS_DIR,
    MONGO_DB,
    MONGO_URI,
    ORDERS_RAW_FILE,
    POSTGRES_DSN,
    QUARANTINE_DIR,
    QUARANTINE_THRESHOLD_PCT,
    ensure_directories,
    get_logger,
)
from src.extract import extract_all
from src.generate_sample_data import generate_all
from src.load_mongodb import (
    get_mongo_collection_counts,
    get_mongo_database,
    load_to_mongodb,
)
from src.load_postgres import (
    get_postgres_connection,
    get_postgres_table_counts,
    load_to_postgres,
    record_batch_run,
)
from src.transform import (
    transform_activity_logs,
    transform_customers,
    transform_orders,
)
from src.validate import (
    generate_dq_report,
    validate_activity_logs,
    validate_customers,
    validate_orders,
)


def run_pipeline(
    generate_if_missing: bool = False,
    regenerate: bool = False,
    skip_db_load: bool = False,
    customers_count: int = 10_000,
    orders_count: int = 25_000,
    activity_count: int = 20_000,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """Execute the complete Extract -> Validate -> Transform -> Load pipeline."""
    ensure_directories()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    batch_id = f"batch_{run_ts}"
    log_file = LOGS_DIR / f"pipeline_{run_ts}.log"
    logger = get_logger("pipeline", log_file=log_file)

    started_at = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    logger.info("Starting ETL pipeline batch_id=%s", batch_id)

    raw_missing = not (
        CUSTOMERS_RAW_FILE.exists()
        and ORDERS_RAW_FILE.exists()
        and ACTIVITY_LOGS_RAW_FILE.exists()
    )
    if regenerate or (generate_if_missing and raw_missing):
        logger.info("Generating synthetic multi-source dataset...")
        generate_all(
            customers_count=customers_count,
            orders_count=orders_count,
            activity_count=activity_count,
        )

    # 1. EXTRACT
    t_ext = time.perf_counter()
    extracted = extract_all()
    extract_seconds = round(time.perf_counter() - t_ext, 3)

    # 2. VALIDATE & QUARANTINE
    t_val = time.perf_counter()
    val_customers = validate_customers(
        extracted["customers"].dataframe,
        timestamp=run_ts,
        quarantine_dir=QUARANTINE_DIR,
    )
    valid_customer_ids = set(val_customers.valid_df["customer_id"].astype(str).str.strip())

    val_orders = validate_orders(
        extracted["orders"].dataframe,
        timestamp=run_ts,
        quarantine_dir=QUARANTINE_DIR,
        valid_customer_ids=valid_customer_ids,
    )
    val_activity = validate_activity_logs(
        extracted["activity_logs"].dataframe,
        timestamp=run_ts,
        quarantine_dir=QUARANTINE_DIR,
    )
    validation_results = {
        "customers": val_customers,
        "orders": val_orders,
        "activity_logs": val_activity,
    }
    validate_seconds = round(time.perf_counter() - t_val, 3)

    # 3. TRANSFORM & PII MASKING
    t_tr = time.perf_counter()
    customers_clean, cust_dupes_removed = transform_customers(val_customers.valid_df)
    orders_clean, order_items_clean, ord_dupes_removed = transform_orders(
        val_orders.valid_df
    )
    activity_clean, act_dupes_removed = transform_activity_logs(val_activity.valid_df)
    transform_seconds = round(time.perf_counter() - t_tr, 3)

    dupes_removed_map = {
        "customers": cust_dupes_removed,
        "orders": ord_dupes_removed,
        "activity_logs": act_dupes_removed,
    }
    total_dupes_removed = sum(dupes_removed_map.values())

    dq_report, dq_report_path = generate_dq_report(
        validation_results,
        batch_id=batch_id,
        timestamp=run_ts,
        logs_dir=LOGS_DIR,
        duplicates_removed_by_source=dupes_removed_map,
    )

    quarantine_pct = dq_report["summary"]["quarantine_rate_pct"]
    if quarantine_pct > QUARANTINE_THRESHOLD_PCT:
        raise RuntimeError(
            f"Quarantine rate {quarantine_pct}% exceeded threshold {QUARANTINE_THRESHOLD_PCT}%"
        )

    # 4. LOAD (PostgreSQL + MongoDB)
    pg_stats: dict[str, int] = {
        "dim_customers": 0,
        "fact_orders": 0,
        "fact_order_items": 0,
        "total_upserted": 0,
    }
    mongo_stats: dict[str, int] = {
        "activity_logs_upserted": 0,
        "audit_reports_upserted": 0,
    }
    pg_table_counts: dict[str, int] = {}
    mongo_collection_counts: dict[str, int] = {}
    load_pg_seconds = 0.0
    load_mongo_seconds = 0.0

    if not skip_db_load:
        t_pg = time.perf_counter()
        pg_stats = load_to_postgres(
            customers_clean,
            orders_clean,
            order_items_clean,
            dsn=POSTGRES_DSN,
            batch_size=batch_size,
        )
        load_pg_seconds = round(time.perf_counter() - t_pg, 3)

        t_mg = time.perf_counter()
        prelim_metrics = {
            "extract_seconds": extract_seconds,
            "validate_seconds": validate_seconds,
            "transform_seconds": transform_seconds,
            "load_postgres_seconds": load_pg_seconds,
        }
        mongo_stats = load_to_mongodb(
            activity_clean,
            dq_report=dq_report,
            run_metrics=prelim_metrics,
            uri=MONGO_URI,
            db_name=MONGO_DB,
            batch_size=batch_size,
        )
        load_mongo_seconds = round(time.perf_counter() - t_mg, 3)

        completed_at = datetime.now(timezone.utc)
        total_seconds = round(time.perf_counter() - t0, 3)
        batch_meta = {
            "batch_id": batch_id,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": total_seconds,
            "status": "SUCCESS",
            "total_records_read": dq_report["summary"]["total_rows_read"],
            "valid_records": dq_report["summary"]["total_valid_rows"],
            "quarantined_records": dq_report["summary"]["total_quarantined_rows"],
            "duplicates_removed": total_dupes_removed,
            "postgres_rows_loaded": pg_stats["total_upserted"],
            "mongo_docs_upserted": mongo_stats["activity_logs_upserted"],
            "dq_report_path": str(dq_report_path),
        }
        pg_conn = get_postgres_connection(POSTGRES_DSN)
        try:
            record_batch_run(pg_conn, batch_meta)
            pg_table_counts = get_postgres_table_counts(pg_conn)
        finally:
            pg_conn.close()

        mongo_client, mongo_db = get_mongo_database(uri=MONGO_URI, db_name=MONGO_DB)
        try:
            mongo_collection_counts = get_mongo_collection_counts(mongo_db)
        finally:
            mongo_client.close()
    else:
        logger.info("Skipping database load (--skip-db-load enabled).")

    total_seconds = round(time.perf_counter() - t0, 3)
    summary = {
        "batch_id": batch_id,
        "status": "SUCCESS",
        "total_records_read": dq_report["summary"]["total_rows_read"],
        "valid_records_passed": dq_report["summary"]["total_valid_rows"],
        "quarantined_records": dq_report["summary"]["total_quarantined_rows"],
        "duplicates_removed": total_dupes_removed,
        "transformed_counts": {
            "dim_customers": len(customers_clean),
            "fact_orders": len(orders_clean),
            "fact_order_items": len(order_items_clean),
            "activity_logs": len(activity_clean),
        },
        "postgres_upserted": pg_stats,
        "postgres_table_counts": pg_table_counts,
        "mongodb_upserted": mongo_stats,
        "mongodb_collection_counts": mongo_collection_counts,
        "timings_seconds": {
            "extract": extract_seconds,
            "validate": validate_seconds,
            "transform": transform_seconds,
            "load_postgres": load_pg_seconds,
            "load_mongodb": load_mongo_seconds,
            "total": total_seconds,
        },
        "dq_report_path": str(dq_report_path),
        "log_file": str(log_file),
    }
    logger.info("Pipeline completed successfully in %.3fs", total_seconds)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Multi-Source ETL & Data Quality Pipeline."
    )
    parser.add_argument(
        "--generate-if-missing",
        action="store_true",
        help="Generate synthetic data if raw files do not exist.",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force regeneration of synthetic data before running.",
    )
    parser.add_argument(
        "--skip-db-load",
        action="store_true",
        help="Run Extract, Validate, and Transform without writing to databases.",
    )
    parser.add_argument("--customers-count", type=int, default=10_000)
    parser.add_argument("--orders-count", type=int, default=25_000)
    parser.add_argument("--activity-count", type=int, default=20_000)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    try:
        result = run_pipeline(
            generate_if_missing=args.generate_if_missing,
            regenerate=args.regenerate,
            skip_db_load=args.skip_db_load,
            customers_count=args.customers_count,
            orders_count=args.orders_count,
            activity_count=args.activity_count,
            batch_size=args.batch_size,
        )
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: ETL pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
