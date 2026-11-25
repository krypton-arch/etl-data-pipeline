"""Data quality & schema validation with quarantine routing and DQ reporting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import (
    ALLOWED_CURRENCIES,
    ALLOWED_EVENT_TYPES,
    ALLOWED_KYC_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    EMAIL_REGEX,
    IPV4_REGEX,
    LOGS_DIR,
    MAX_LATENCY_MS,
    MAX_ORDER_AMOUNT,
    MIN_LATENCY_MS,
    MIN_ORDER_AMOUNT,
    PHONE_REGEX,
    QUARANTINE_DIR,
    SCHEMA_RULES,
    get_logger,
)

logger = get_logger("validate")


@dataclass
class ValidationResult:
    """Result of validating a single source dataset."""

    source_name: str
    total_rows: int
    valid_df: pd.DataFrame
    quarantined_df: pd.DataFrame
    quarantine_file: Path | None
    exact_duplicates: int
    pk_duplicates: int
    null_percentages: dict[str, float]
    rejection_breakdown: dict[str, int] = field(default_factory=dict)


def _is_missing(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _count_exact_duplicates(df: pd.DataFrame) -> int:
    """Count exact duplicate rows safely even when nested list/dict columns exist."""
    if df.empty:
        return 0
    comparable = pd.DataFrame(index=df.index)
    for col in df.columns:
        series = df[col]
        if series.apply(lambda v: isinstance(v, (list, dict))).any():
            comparable[col] = series.apply(
                lambda v: json.dumps(v, sort_keys=True, default=str)
                if isinstance(v, (list, dict))
                else v
            )
        else:
            comparable[col] = series
    return int(comparable.duplicated(keep="first").sum())


def _compute_null_percentages(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {col: 0.0 for col in df.columns}
    percentages: dict[str, float] = {}
    total = len(df)
    for col in df.columns:
        null_count = int(df[col].apply(_is_missing).sum())
        percentages[col] = round((null_count / total) * 100.0, 2)
    return percentages


def _check_required_schema(df: pd.DataFrame, source_name: str) -> None:
    rules = SCHEMA_RULES[source_name]
    missing_cols = [c for c in rules["required_columns"] if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Schema mismatch for source '{source_name}': missing required columns {missing_cols}"
        )


def _write_quarantine(
    quarantined_df: pd.DataFrame,
    source_name: str,
    timestamp: str,
    quarantine_dir: Path = QUARANTINE_DIR,
) -> Path | None:
    if quarantined_df.empty:
        return None
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    out_path = quarantine_dir / f"{source_name}_quarantine_{timestamp}.json"

    records = json.loads(quarantined_df.to_json(orient="records", date_format="iso"))
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)
    logger.warning(
        "Quarantined %d invalid '%s' records -> %s",
        len(quarantined_df),
        source_name,
        out_path,
    )
    return out_path


def _split_and_finalize(
    df: pd.DataFrame,
    reasons_list: list[list[str]],
    source_name: str,
    timestamp: str,
    quarantine_dir: Path,
) -> ValidationResult:
    rules = SCHEMA_RULES[source_name]
    pk_col = rules["primary_key"]

    exact_dupes = _count_exact_duplicates(df)
    non_null_pk = df[~df[pk_col].apply(_is_missing)]
    pk_dupes = int(non_null_pk.duplicated(subset=[pk_col], keep="last").sum())
    null_pcts = _compute_null_percentages(df)

    is_invalid = pd.Series([len(r) > 0 for r in reasons_list], index=df.index)
    valid_df = df.loc[~is_invalid].copy().reset_index(drop=True)
    quarantined_df = df.loc[is_invalid].copy()

    breakdown: dict[str, int] = {}
    if not quarantined_df.empty:
        joined_reasons: list[str] = []
        for idx in quarantined_df.index:
            pos = df.index.get_loc(idx)
            row_reasons = reasons_list[pos]
            for reason in row_reasons:
                breakdown[reason] = breakdown.get(reason, 0) + 1
            joined_reasons.append("; ".join(row_reasons))
        quarantined_df["rejection_reason"] = joined_reasons
        quarantined_df["quarantined_at"] = datetime.now(timezone.utc).isoformat()
        quarantined_df = quarantined_df.reset_index(drop=True)

    quarantine_file = _write_quarantine(
        quarantined_df, source_name, timestamp, quarantine_dir=quarantine_dir
    )

    logger.info(
        "Validated '%s': total=%d, valid=%d, quarantined=%d, exact_dupes=%d, pk_dupes=%d",
        source_name,
        len(df),
        len(valid_df),
        len(quarantined_df),
        exact_dupes,
        pk_dupes,
    )
    return ValidationResult(
        source_name=source_name,
        total_rows=len(df),
        valid_df=valid_df,
        quarantined_df=quarantined_df,
        quarantine_file=quarantine_file,
        exact_duplicates=exact_dupes,
        pk_duplicates=pk_dupes,
        null_percentages=null_pcts,
        rejection_breakdown=breakdown,
    )


def validate_customers(
    df: pd.DataFrame,
    timestamp: str,
    quarantine_dir: Path = QUARANTINE_DIR,
) -> ValidationResult:
    """Validate customers schema, required fields, regex formats, dates, and KYC enum."""
    _check_required_schema(df, "customers")
    work = df.copy()
    reasons: list[list[str]] = [[] for _ in range(len(work))]

    parsed_dates = pd.to_datetime(
        work["signup_date"], utc=True, format="mixed", errors="coerce"
    )

    for i, row in enumerate(work.itertuples(index=False)):
        row_dict = row._asdict()
        for col in SCHEMA_RULES["customers"]["non_null_columns"]:
            if _is_missing(row_dict.get(col)):
                reasons[i].append(f"null_{col}")

        email = row_dict.get("email")
        if not _is_missing(email) and not EMAIL_REGEX.match(str(email).strip()):
            reasons[i].append("invalid_email_format")

        phone = row_dict.get("phone")
        if not _is_missing(phone):
            phone_str = str(phone).strip()
            digits = "".join(ch for ch in phone_str if ch.isdigit())
            if not PHONE_REGEX.match(phone_str) or not (7 <= len(digits) <= 15):
                reasons[i].append("invalid_phone_format")

        if not _is_missing(row_dict.get("signup_date")) and pd.isna(parsed_dates.iloc[i]):
            reasons[i].append("invalid_signup_date")

        kyc = row_dict.get("kyc_status")
        if not _is_missing(kyc) and str(kyc).strip().lower() not in ALLOWED_KYC_STATUSES:
            reasons[i].append("invalid_kyc_status")

    return _split_and_finalize(work, reasons, "customers", timestamp, quarantine_dir)


def validate_orders(
    df: pd.DataFrame,
    timestamp: str,
    quarantine_dir: Path = QUARANTINE_DIR,
    valid_customer_ids: set[str] | None = None,
) -> ValidationResult:
    """Validate orders schema, amounts, timestamps, currencies, nested items, and FK presence."""
    _check_required_schema(df, "orders")
    work = df.copy()
    reasons: list[list[str]] = [[] for _ in range(len(work))]

    parsed_dates = pd.to_datetime(
        work["order_timestamp"], utc=True, format="mixed", errors="coerce"
    )
    numeric_amounts = pd.to_numeric(work["amount"], errors="coerce")

    for i in range(len(work)):
        row = work.iloc[i]
        for col in SCHEMA_RULES["orders"]["non_null_columns"]:
            if _is_missing(row.get(col)):
                reasons[i].append(f"null_{col}")

        cid = row.get("customer_id")
        if (
            valid_customer_ids is not None
            and not _is_missing(cid)
            and str(cid).strip() not in valid_customer_ids
        ):
            reasons[i].append("orphan_customer_id")

        if not _is_missing(row.get("order_timestamp")) and pd.isna(parsed_dates.iloc[i]):
            reasons[i].append("invalid_order_timestamp")

        if not _is_missing(row.get("amount")):
            amt = numeric_amounts.iloc[i]
            if pd.isna(amt):
                reasons[i].append("non_numeric_amount")
            elif amt < MIN_ORDER_AMOUNT or amt > MAX_ORDER_AMOUNT:
                reasons[i].append("amount_out_of_range")

        curr = row.get("currency")
        if not _is_missing(curr) and str(curr).strip().upper() not in ALLOWED_CURRENCIES:
            reasons[i].append("unsupported_currency")

        pm = row.get("payment_method")
        if not _is_missing(pm) and str(pm).strip().lower() not in ALLOWED_PAYMENT_METHODS:
            reasons[i].append("unsupported_payment_method")

        items = row.get("items")
        if not isinstance(items, list) or len(items) == 0:
            reasons[i].append("invalid_or_empty_items")
        else:
            bad_item = False
            for item in items:
                if not isinstance(item, dict):
                    bad_item = True
                    break
                qty = item.get("quantity")
                price = item.get("unit_price")
                if (
                    not isinstance(qty, (int, float))
                    or qty <= 0
                    or not isinstance(price, (int, float))
                    or price < 0
                ):
                    bad_item = True
                    break
            if bad_item:
                reasons[i].append("invalid_item_attributes")

        addr = row.get("shipping_address")
        if not isinstance(addr, dict):
            reasons[i].append("invalid_shipping_address")

    return _split_and_finalize(work, reasons, "orders", timestamp, quarantine_dir)


def validate_activity_logs(
    df: pd.DataFrame,
    timestamp: str,
    quarantine_dir: Path = QUARANTINE_DIR,
) -> ValidationResult:
    """Validate activity log schema, IPv4 format, latency bounds, event type, and timestamps."""
    _check_required_schema(df, "activity_logs")
    work = df.copy()
    reasons: list[list[str]] = [[] for _ in range(len(work))]

    parsed_dates = pd.to_datetime(
        work["event_timestamp"], utc=True, format="mixed", errors="coerce"
    )
    numeric_latency = pd.to_numeric(work["latency_ms"], errors="coerce")

    for i, row in enumerate(work.itertuples(index=False)):
        row_dict = row._asdict()
        for col in SCHEMA_RULES["activity_logs"]["non_null_columns"]:
            if _is_missing(row_dict.get(col)):
                reasons[i].append(f"null_{col}")

        ip = row_dict.get("ip_address")
        if not _is_missing(ip) and not IPV4_REGEX.match(str(ip).strip()):
            reasons[i].append("invalid_ip_address")

        if not _is_missing(row_dict.get("latency_ms")):
            lat = numeric_latency.iloc[i]
            if pd.isna(lat):
                reasons[i].append("non_numeric_latency")
            elif lat < MIN_LATENCY_MS or lat > MAX_LATENCY_MS:
                reasons[i].append("latency_out_of_range")

        if not _is_missing(row_dict.get("event_timestamp")) and pd.isna(parsed_dates.iloc[i]):
            reasons[i].append("invalid_event_timestamp")

        evt = row_dict.get("event_type")
        if not _is_missing(evt) and str(evt).strip().lower() not in ALLOWED_EVENT_TYPES:
            reasons[i].append("invalid_event_type")

    return _split_and_finalize(work, reasons, "activity_logs", timestamp, quarantine_dir)


def generate_dq_report(
    results: dict[str, ValidationResult],
    batch_id: str,
    timestamp: str,
    logs_dir: Path = LOGS_DIR,
    duplicates_removed_by_source: dict[str, int] | None = None,
) -> tuple[dict[str, Any], Path]:
    """Build and persist structured JSON Data Quality Report."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    report_path = logs_dir / f"dq_report_{timestamp}.json"

    total_read = sum(r.total_rows for r in results.values())
    total_valid = sum(len(r.valid_df) for r in results.values())
    total_quarantined = sum(len(r.quarantined_df) for r in results.values())
    total_exact_dupes = sum(r.exact_duplicates for r in results.values())
    total_pk_dupes = sum(r.pk_duplicates for r in results.values())

    pass_rate = round((total_valid / total_read) * 100.0, 2) if total_read else 100.0
    quarantine_rate = round((total_quarantined / total_read) * 100.0, 2) if total_read else 0.0

    sources_summary: dict[str, Any] = {}
    for name, res in results.items():
        s_read = res.total_rows
        s_valid = len(res.valid_df)
        s_quar = len(res.quarantined_df)
        sources_summary[name] = {
            "rows_read": s_read,
            "valid_rows": s_valid,
            "quarantined_rows": s_quar,
            "pass_rate_pct": round((s_valid / s_read) * 100.0, 2) if s_read else 100.0,
            "exact_duplicates": res.exact_duplicates,
            "pk_duplicates": res.pk_duplicates,
            "duplicates_removed_in_transform": (
                duplicates_removed_by_source.get(name, 0)
                if duplicates_removed_by_source
                else 0
            ),
            "null_percentages": res.null_percentages,
            "rejection_breakdown": res.rejection_breakdown,
            "quarantine_file": str(res.quarantine_file) if res.quarantine_file else None,
        }

    report = {
        "batch_id": batch_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_rows_read": total_read,
            "total_valid_rows": total_valid,
            "total_quarantined_rows": total_quarantined,
            "overall_pass_rate_pct": pass_rate,
            "quarantine_rate_pct": quarantine_rate,
            "total_exact_duplicates_detected": total_exact_dupes,
            "total_pk_duplicates_detected": total_pk_dupes,
        },
        "sources": sources_summary,
    }

    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    logger.info("Data Quality Report written -> %s", report_path)
    return report, report_path
