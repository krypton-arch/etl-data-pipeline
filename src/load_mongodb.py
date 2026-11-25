"""Idempotent MongoDB bulk loader for semi-structured activity logs and audit reports."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.database import Database

from config.settings import BATCH_SIZE, MONGO_DB, MONGO_URI, get_logger

logger = get_logger("load_mongodb")


def get_mongo_database(
    uri: str = MONGO_URI,
    db_name: str = MONGO_DB,
) -> tuple[MongoClient, Database]:
    """Connect to MongoDB and return (client, database)."""
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client, client[db_name]


def ensure_mongo_indexes(db: Database) -> None:
    """Create unique and compound indexes on activity_logs and etl_audit_reports."""
    activity_col = db["activity_logs"]
    activity_col.create_index([("event_id", ASCENDING)], unique=True, name="uq_event_id")
    activity_col.create_index(
        [("customer_id", ASCENDING), ("event_timestamp", DESCENDING)],
        name="idx_customer_event_ts",
    )
    activity_col.create_index(
        [("event_type", ASCENDING), ("device_os", ASCENDING)],
        name="idx_event_type_os",
    )

    audit_col = db["etl_audit_reports"]
    audit_col.create_index([("batch_id", ASCENDING)], unique=True, name="uq_batch_id")
    audit_col.create_index([("generated_at", DESCENDING)], name="idx_generated_at")
    logger.info("Verified MongoDB indexes on activity_logs and etl_audit_reports")


def upsert_activity_logs(
    db: Database,
    activity_df: pd.DataFrame,
    batch_id: str,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Bulk upsert activity log documents into MongoDB using UpdateOne(..., upsert=True)."""
    if activity_df.empty:
        return 0

    col = db["activity_logs"]
    ingested_at = datetime.now(timezone.utc).isoformat()
    total_processed = 0

    records = activity_df.to_dict(orient="records")
    for start in range(0, len(records), batch_size):
        chunk = records[start : start + batch_size]
        ops: list[UpdateOne] = []
        for rec in chunk:
            doc = {
                "event_id": rec["event_id"],
                "customer_id": rec["customer_id"],
                "event_type": rec["event_type"],
                "network": {
                    "ip_masked": rec["ip_masked"],
                    "ip_hash": rec["ip_hash"],
                },
                "device_os": rec["device_os"],
                "latency_ms": int(rec["latency_ms"]),
                "event_timestamp": rec["event_timestamp"],
                "etl_batch_id": batch_id,
                "ingested_at": ingested_at,
            }
            ops.append(
                UpdateOne(
                    {"event_id": doc["event_id"]},
                    {"$set": doc},
                    upsert=True,
                )
            )
        if ops:
            col.bulk_write(ops, ordered=False)
            total_processed += len(ops)

    logger.info("Bulk upserted %d documents into MongoDB 'activity_logs'", total_processed)
    return total_processed


def upsert_audit_report(
    db: Database,
    dq_report: dict[str, Any],
    run_metrics: dict[str, Any],
) -> None:
    """Upsert ETL batch audit and Data Quality report into etl_audit_reports."""
    col = db["etl_audit_reports"]
    audit_doc = {
        "batch_id": dq_report["batch_id"],
        "generated_at": dq_report["generated_at"],
        "summary": dq_report["summary"],
        "sources": dq_report["sources"],
        "execution_metrics": run_metrics,
    }
    col.update_one(
        {"batch_id": audit_doc["batch_id"]},
        {"$set": audit_doc},
        upsert=True,
    )
    logger.info(
        "Upserted audit report for batch '%s' into MongoDB 'etl_audit_reports'",
        audit_doc["batch_id"],
    )


def get_mongo_collection_counts(db: Database) -> dict[str, int]:
    """Return document counts for MongoDB collections."""
    return {
        "activity_logs": db["activity_logs"].count_documents({}),
        "etl_audit_reports": db["etl_audit_reports"].count_documents({}),
    }


def load_to_mongodb(
    activity_df: pd.DataFrame,
    dq_report: dict[str, Any],
    run_metrics: dict[str, Any],
    uri: str = MONGO_URI,
    db_name: str = MONGO_DB,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    """Connect, ensure indexes, and bulk upsert activity logs and audit report."""
    client, db = get_mongo_database(uri=uri, db_name=db_name)
    try:
        ensure_mongo_indexes(db)
        activity_count = upsert_activity_logs(
            db,
            activity_df,
            batch_id=dq_report["batch_id"],
            batch_size=batch_size,
        )
        upsert_audit_report(db, dq_report, run_metrics)
        return {
            "activity_logs_upserted": activity_count,
            "audit_reports_upserted": 1,
        }
    finally:
        client.close()
