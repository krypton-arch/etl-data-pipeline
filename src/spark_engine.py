"""PySpark execution engine for Extract -> Validate -> Transform -> PII Masking.

Uses JVM-native Spark SQL expressions (avoiding Python UDF worker serialization)
so distributed transformations execute cleanly across both Linux clusters and Windows.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config.settings import (
    ACTIVITY_LOGS_RAW_FILE,
    ALLOWED_CURRENCIES,
    ALLOWED_EVENT_TYPES,
    ALLOWED_KYC_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    CUSTOMERS_RAW_FILE,
    LOGS_DIR,
    MAX_LATENCY_MS,
    MAX_ORDER_AMOUNT,
    MIN_LATENCY_MS,
    MIN_ORDER_AMOUNT,
    ORDERS_RAW_FILE,
    PII_HASH_SALT,
    QUARANTINE_DIR,
    SCHEMA_RULES,
    get_logger,
)
from src.validate import ValidationResult, generate_dq_report

logger = get_logger("spark_engine")

_EMAIL_SPARK_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
_PHONE_SPARK_REGEX = r"^\+?[0-9\-\s\(\)]{7,20}$"
_IPV4_SPARK_REGEX = (
    r"^(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}"
    r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$"
)


def create_spark_session(app_name: str = "MultiSourceETLPipeline") -> SparkSession:
    """Create and configure a SparkSession with ANSI off for safe dirty-data casting."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    master = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Initialized SparkSession (version=%s, master=%s)", spark.version, master)
    return spark


def _is_missing_expr(col_name: str):
    c = F.col(col_name).cast("string")
    return (
        F.col(col_name).isNull()
        | (F.trim(c) == "")
        | F.lower(F.trim(c)).isin("null", "none", "nan", "n/a")
    )


def _parse_mixed_utc_timestamp(col_name: str):
    trimmed = F.trim(F.col(col_name).cast("string"))
    return F.coalesce(
        F.expr(f"try_to_timestamp(trim(cast(`{col_name}` as string)))"),
        F.to_timestamp(trimmed, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        F.to_timestamp(trimmed, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(trimmed, "yyyy-MM-dd HH:mm:ss"),
    )


def _check_required_columns(df: DataFrame, source_name: str) -> None:
    required = SCHEMA_RULES[source_name]["required_columns"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Schema mismatch for source '{source_name}': missing required columns {missing}"
        )


def _compute_spark_metrics_and_quarantine(
    raw_df: DataFrame,
    flagged_df: DataFrame,
    source_name: str,
    timestamp: str,
    quarantine_dir: Path,
) -> tuple[DataFrame, ValidationResult]:
    pk_col = SCHEMA_RULES[source_name]["primary_key"]
    total_rows = raw_df.count()

    # Exact duplicates (cast nested columns to JSON string if needed)
    comparable_cols = [
        F.to_json(F.col(c)).alias(c)
        if raw_df.schema[c].dataType.typeName() in ("array", "struct", "map")
        else F.col(c)
        for c in raw_df.columns
    ]
    exact_unique = raw_df.select(*comparable_cols).dropDuplicates().count()
    exact_dupes = total_rows - exact_unique

    non_null_pk_df = raw_df.filter(~_is_missing_expr(pk_col))
    pk_total = non_null_pk_df.count()
    pk_unique = non_null_pk_df.select(F.trim(F.col(pk_col))).distinct().count()
    pk_dupes = pk_total - pk_unique

    # Null percentages across raw columns
    null_percentages: dict[str, float] = {}
    if total_rows > 0:
        agg_exprs = [
            F.sum(F.when(_is_missing_expr(c), 1).otherwise(0)).alias(c)
            if raw_df.schema[c].dataType.typeName() not in ("array", "struct", "map")
            else F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in raw_df.columns
        ]
        null_counts = raw_df.agg(*agg_exprs).collect()[0].asDict()
        for c, cnt in null_counts.items():
            null_percentages[c] = round((int(cnt or 0) / total_rows) * 100.0, 2)
    else:
        null_percentages = {c: 0.0 for c in raw_df.columns}

    valid_spark_df = flagged_df.filter(F.size(F.col("_reasons")) == 0).drop("_reasons")
    quarantined_spark_df = flagged_df.filter(F.size(F.col("_reasons")) > 0)

    quarantined_count = quarantined_spark_df.count()
    valid_count = total_rows - quarantined_count

    breakdown: dict[str, int] = {}
    quarantine_file: Path | None = None
    quarantined_pdf = pd.DataFrame()

    if quarantined_count > 0:
        reason_rows = (
            quarantined_spark_df.select(F.explode(F.col("_reasons")).alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
        breakdown = {r["reason"]: int(r["count"]) for r in reason_rows}

        q_export = (
            quarantined_spark_df.withColumn(
                "rejection_reason", F.concat_ws("; ", F.col("_reasons"))
            )
            .withColumn(
                "quarantined_at",
                F.lit(datetime.now(timezone.utc).isoformat()),
            )
            .drop("_reasons", "_parsed_ts", "_parsed_amount", "_parsed_latency")
        )
        quarantined_pdf = q_export.toPandas()
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_file = quarantine_dir / f"{source_name}_quarantine_{timestamp}.json"
        records = json.loads(quarantined_pdf.to_json(orient="records", date_format="iso"))
        with quarantine_file.open("w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2)
        logger.warning(
            "[PySpark] Quarantined %d invalid '%s' records -> %s",
            quarantined_count,
            source_name,
            quarantine_file,
        )

    # Lightweight placeholder DataFrames in ValidationResult for generate_dq_report compatibility
    res = ValidationResult(
        source_name=source_name,
        total_rows=total_rows,
        valid_df=pd.DataFrame(index=range(valid_count)),
        quarantined_df=quarantined_pdf
        if not quarantined_pdf.empty
        else pd.DataFrame(index=range(quarantined_count)),
        quarantine_file=quarantine_file,
        exact_duplicates=exact_dupes,
        pk_duplicates=pk_dupes,
        null_percentages=null_percentages,
        rejection_breakdown=breakdown,
    )
    logger.info(
        "[PySpark] Validated '%s': total=%d, valid=%d, quarantined=%d, exact_dupes=%d, pk_dupes=%d",
        source_name,
        total_rows,
        valid_count,
        quarantined_count,
        exact_dupes,
        pk_dupes,
    )
    return valid_spark_df, res


def _dedupe_spark(df: DataFrame, pk_col: str, ts_col_expr: str = "_parsed_ts") -> tuple[DataFrame, int]:
    before = df.count()
    if before == 0:
        return df, 0
    win = Window.partitionBy(F.trim(F.col(pk_col))).orderBy(
        F.col(ts_col_expr).desc_nulls_last(), F.trim(F.col(pk_col)).desc()
    )
    deduped = df.withColumn("_rn", F.row_number().over(win)).filter(F.col("_rn") == 1).drop("_rn")
    after = deduped.count()
    return deduped, before - after


def run_spark_etl_stages(
    spark: SparkSession,
    batch_id: str,
    run_ts: str,
    customers_path: Path = CUSTOMERS_RAW_FILE,
    orders_path: Path = ORDERS_RAW_FILE,
    activity_path: Path = ACTIVITY_LOGS_RAW_FILE,
    quarantine_dir: Path = QUARANTINE_DIR,
    logs_dir: Path = LOGS_DIR,
    salt: str = PII_HASH_SALT,
) -> dict[str, Any]:
    """Run Extract, Validate/Quarantine, and Transform/PII Masking in PySpark."""
    # 1. EXTRACT
    t_ext = time.perf_counter()
    cust_raw = (
        spark.read.option("header", "true")
        .option("nullValue", "")
        .csv(str(customers_path))
    )
    orders_raw = spark.read.option("multiLine", "true").json(str(orders_path))
    activity_raw = spark.read.parquet(str(activity_path))

    # Materialize extract counts once
    cust_raw.cache()
    orders_raw.cache()
    activity_raw.cache()
    _ = cust_raw.count(), orders_raw.count(), activity_raw.count()
    extract_seconds = round(time.perf_counter() - t_ext, 3)
    logger.info("[PySpark] Extracted all 3 sources in %.3fs", extract_seconds)

    # 2. VALIDATE & QUARANTINE
    t_val = time.perf_counter()
    _check_required_columns(cust_raw, "customers")
    _check_required_columns(orders_raw, "orders")
    _check_required_columns(activity_raw, "activity_logs")

    # Customers validation
    cust_prep = cust_raw.withColumn("_parsed_ts", _parse_mixed_utc_timestamp("signup_date"))
    phone_digits = F.regexp_replace(F.col("phone").cast("string"), r"[^0-9]", "")
    cust_reasons = F.array_compact(
        F.array(
            *[
                F.when(_is_missing_expr(c), F.lit(f"null_{c}"))
                for c in SCHEMA_RULES["customers"]["non_null_columns"]
            ],
            F.when(
                (~_is_missing_expr("email"))
                & (~F.trim(F.col("email")).rlike(_EMAIL_SPARK_REGEX)),
                F.lit("invalid_email_format"),
            ),
            F.when(
                (~_is_missing_expr("phone"))
                & (
                    (~F.trim(F.col("phone")).rlike(_PHONE_SPARK_REGEX))
                    | (F.length(phone_digits) < 7)
                    | (F.length(phone_digits) > 15)
                ),
                F.lit("invalid_phone_format"),
            ),
            F.when(
                (~_is_missing_expr("signup_date")) & F.col("_parsed_ts").isNull(),
                F.lit("invalid_signup_date"),
            ),
            F.when(
                (~_is_missing_expr("kyc_status"))
                & (~F.lower(F.trim(F.col("kyc_status"))).isin(list(ALLOWED_KYC_STATUSES))),
                F.lit("invalid_kyc_status"),
            ),
        )
    )
    cust_flagged = cust_prep.withColumn("_reasons", cust_reasons)
    cust_valid_spark, val_cust_res = _compute_spark_metrics_and_quarantine(
        cust_raw, cust_flagged, "customers", run_ts, quarantine_dir
    )

    valid_cids_df = cust_valid_spark.select(
        F.trim(F.col("customer_id")).alias("_valid_cid")
    ).distinct()

    # Orders validation (including distributed FK check against valid customers)
    orders_prep = (
        orders_raw.withColumn("_parsed_ts", _parse_mixed_utc_timestamp("order_timestamp"))
        .withColumn("_parsed_amount", F.expr("try_cast(amount as double)"))
        .join(
            F.broadcast(valid_cids_df),
            F.trim(F.col("customer_id")) == F.col("_valid_cid"),
            how="left",
        )
    )
    bad_items_expr = (
        F.col("items").isNull()
        | (F.size(F.col("items")) == 0)
        | F.exists(
            F.col("items"),
            lambda item: (item["quantity"].isNull())
            | (item["quantity"] <= 0)
            | (item["unit_price"].isNull())
            | (item["unit_price"] < 0),
        )
    )
    orders_reasons = F.array_compact(
        F.array(
            *[
                F.when(_is_missing_expr(c), F.lit(f"null_{c}"))
                for c in SCHEMA_RULES["orders"]["non_null_columns"]
            ],
            F.when(
                (~_is_missing_expr("customer_id")) & F.col("_valid_cid").isNull(),
                F.lit("orphan_customer_id"),
            ),
            F.when(
                (~_is_missing_expr("order_timestamp")) & F.col("_parsed_ts").isNull(),
                F.lit("invalid_order_timestamp"),
            ),
            F.when(
                (~_is_missing_expr("amount")) & F.col("_parsed_amount").isNull(),
                F.lit("non_numeric_amount"),
            ),
            F.when(
                (~_is_missing_expr("amount"))
                & F.col("_parsed_amount").isNotNull()
                & (
                    (F.col("_parsed_amount") < MIN_ORDER_AMOUNT)
                    | (F.col("_parsed_amount") > MAX_ORDER_AMOUNT)
                ),
                F.lit("amount_out_of_range"),
            ),
            F.when(
                (~_is_missing_expr("currency"))
                & (~F.upper(F.trim(F.col("currency"))).isin(list(ALLOWED_CURRENCIES))),
                F.lit("unsupported_currency"),
            ),
            F.when(
                (~_is_missing_expr("payment_method"))
                & (~F.lower(F.trim(F.col("payment_method"))).isin(list(ALLOWED_PAYMENT_METHODS))),
                F.lit("unsupported_payment_method"),
            ),
            F.when(bad_items_expr, F.lit("invalid_or_empty_items")),
            F.when(F.col("shipping_address").isNull(), F.lit("invalid_shipping_address")),
        )
    )
    orders_flagged = orders_prep.withColumn("_reasons", orders_reasons).drop("_valid_cid")
    orders_valid_spark, val_orders_res = _compute_spark_metrics_and_quarantine(
        orders_raw, orders_flagged, "orders", run_ts, quarantine_dir
    )

    # Activity logs validation
    act_prep = activity_raw.withColumn(
        "_parsed_ts", _parse_mixed_utc_timestamp("event_timestamp")
    ).withColumn("_parsed_latency", F.expr("try_cast(latency_ms as double)"))
    act_reasons = F.array_compact(
        F.array(
            *[
                F.when(_is_missing_expr(c), F.lit(f"null_{c}"))
                for c in SCHEMA_RULES["activity_logs"]["non_null_columns"]
            ],
            F.when(
                (~_is_missing_expr("ip_address"))
                & (~F.trim(F.col("ip_address")).rlike(_IPV4_SPARK_REGEX)),
                F.lit("invalid_ip_address"),
            ),
            F.when(
                (~_is_missing_expr("latency_ms")) & F.col("_parsed_latency").isNull(),
                F.lit("non_numeric_latency"),
            ),
            F.when(
                (~_is_missing_expr("latency_ms"))
                & F.col("_parsed_latency").isNotNull()
                & (
                    (F.col("_parsed_latency") < MIN_LATENCY_MS)
                    | (F.col("_parsed_latency") > MAX_LATENCY_MS)
                ),
                F.lit("latency_out_of_range"),
            ),
            F.when(
                (~_is_missing_expr("event_timestamp")) & F.col("_parsed_ts").isNull(),
                F.lit("invalid_event_timestamp"),
            ),
            F.when(
                (~_is_missing_expr("event_type"))
                & (~F.lower(F.trim(F.col("event_type"))).isin(list(ALLOWED_EVENT_TYPES))),
                F.lit("invalid_event_type"),
            ),
        )
    )
    act_flagged = act_prep.withColumn("_reasons", act_reasons)
    act_valid_spark, val_act_res = _compute_spark_metrics_and_quarantine(
        activity_raw, act_flagged, "activity_logs", run_ts, quarantine_dir
    )
    validate_seconds = round(time.perf_counter() - t_val, 3)

    # 3. TRANSFORM & PII MASKING
    t_tr = time.perf_counter()

    # Customers transform
    cust_deduped, cust_dupes_removed = _dedupe_spark(cust_valid_spark, "customer_id")
    email_norm = F.lower(F.trim(F.col("email")))
    cust_digits = F.regexp_replace(F.col("phone").cast("string"), r"[^0-9]", "")
    cust_city_clean = F.when(
        _is_missing_expr("city"), F.lit("Unknown")
    ).otherwise(F.trim(F.col("city")))

    customers_transformed_spark = cust_deduped.select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.initcap(F.regexp_replace(F.trim(F.col("full_name")), r"\s+", " ")).alias("full_name"),
        F.concat(
            F.substring(email_norm, 1, 1),
            F.lit("***@"),
            F.element_at(F.split(email_norm, "@"), 2),
        ).alias("email_masked"),
        F.sha2(F.concat_ws(":", F.lit(salt), email_norm), 256).alias("email_hash"),
        F.when(
            F.length(cust_digits) <= 4,
            F.repeat(F.lit("X"), F.greatest(F.length(cust_digits), F.lit(4))),
        )
        .otherwise(
            F.concat(
                F.repeat(F.lit("X"), F.greatest(F.length(cust_digits) - 4, F.lit(2))),
                F.expr("substring(regexp_replace(cast(phone as string), '[^0-9]', ''), -4, 4)"),
            )
        )
        .alias("phone_masked"),
        F.sha2(F.concat_ws(":", F.lit(salt), cust_digits), 256).alias("phone_hash"),
        F.initcap(cust_city_clean).alias("city"),
        F.date_format(F.col("_parsed_ts"), "yyyy-MM-dd'T'HH:mm:ss'+00:00'").alias("signup_date"),
        F.lower(F.trim(F.col("kyc_status"))).alias("kyc_status"),
    )

    # Orders & Order Items transform (strip shipping_address.street PII)
    orders_deduped, ord_dupes_removed = _dedupe_spark(orders_valid_spark, "order_id")
    orders_transformed_spark = orders_deduped.select(
        F.trim(F.col("order_id")).alias("order_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.date_format(F.col("_parsed_ts"), "yyyy-MM-dd'T'HH:mm:ss'+00:00'").alias(
            "order_timestamp"
        ),
        F.round(F.col("_parsed_amount"), 2).alias("amount"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),
        F.lower(F.trim(F.col("payment_method"))).alias("payment_method"),
        F.coalesce(
            F.trim(F.col("shipping_address.country")), F.lit("Unknown")
        ).alias("shipping_country"),
        F.initcap(
            F.coalesce(F.trim(F.col("shipping_address.city")), F.lit("Unknown"))
        ).alias("shipping_city"),
        F.coalesce(
            F.trim(F.col("shipping_address.postal_code")), F.lit("00000")
        ).alias("shipping_postal_code"),
        F.size(F.col("items")).cast("int").alias("item_count"),
    )

    order_items_transformed_spark = (
        orders_deduped.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.posexplode(F.col("items")).alias("pos", "item"),
        )
        .select(
            F.concat(F.col("order_id"), F.lit("_"), (F.col("pos") + 1).cast("string")).alias(
                "order_item_id"
            ),
            F.col("order_id"),
            (F.col("pos") + 1).cast("int").alias("line_number"),
            F.coalesce(F.trim(F.col("item.sku")), F.lit("UNKNOWN")).alias("sku"),
            F.coalesce(
                F.trim(F.col("item.product_name")), F.lit("Unknown Product")
            ).alias("product_name"),
            F.coalesce(F.trim(F.col("item.category")), F.lit("General")).alias("category"),
            F.col("item.quantity").cast("int").alias("quantity"),
            F.round(F.col("item.unit_price").cast("double"), 2).alias("unit_price"),
            F.round(
                F.col("item.quantity").cast("double") * F.col("item.unit_price").cast("double"),
                2,
            ).alias("line_total"),
        )
    )

    # Activity logs transform
    act_deduped, act_dupes_removed = _dedupe_spark(act_valid_spark, "event_id")
    ip_trimmed = F.trim(F.col("ip_address"))
    os_clean = F.when(_is_missing_expr("device_os"), F.lit("Other")).otherwise(
        F.trim(F.col("device_os"))
    )
    activity_transformed_spark = act_deduped.select(
        F.trim(F.col("event_id")).alias("event_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type"),
        F.regexp_replace(
            ip_trimmed, r"^([0-9]+\.[0-9]+)\.[0-9]+\.[0-9]+$", "$1.xxx.xxx"
        ).alias("ip_masked"),
        F.sha2(F.concat_ws(":", F.lit(salt), ip_trimmed), 256).alias("ip_hash"),
        os_clean.alias("device_os"),
        F.col("_parsed_latency").cast("int").alias("latency_ms"),
        F.date_format(F.col("_parsed_ts"), "yyyy-MM-dd'T'HH:mm:ss'+00:00'").alias(
            "event_timestamp"
        ),
    )

    customers_clean = customers_transformed_spark.toPandas()
    orders_clean = orders_transformed_spark.toPandas()
    order_items_clean = order_items_transformed_spark.toPandas()
    activity_clean = activity_transformed_spark.toPandas()
    transform_seconds = round(time.perf_counter() - t_tr, 3)

    dupes_removed_map = {
        "customers": cust_dupes_removed,
        "orders": ord_dupes_removed,
        "activity_logs": act_dupes_removed,
    }
    dq_report, dq_report_path = generate_dq_report(
        {
            "customers": val_cust_res,
            "orders": val_orders_res,
            "activity_logs": val_act_res,
        },
        batch_id=batch_id,
        timestamp=run_ts,
        logs_dir=logs_dir,
        duplicates_removed_by_source=dupes_removed_map,
    )

    cust_raw.unpersist()
    orders_raw.unpersist()
    activity_raw.unpersist()

    return {
        "customers_clean": customers_clean,
        "orders_clean": orders_clean,
        "order_items_clean": order_items_clean,
        "activity_clean": activity_clean,
        "dq_report": dq_report,
        "dq_report_path": dq_report_path,
        "dupes_removed": sum(dupes_removed_map.values()),
        "timings": {
            "extract": extract_seconds,
            "validate": validate_seconds,
            "transform": transform_seconds,
        },
    }
