# Multi-Source ETL & Data Quality Pipeline

A production-style **Multi-Source ETL & Data Quality Pipeline** built with **Python 3.11+**, **Pandas**, **PyArrow**, **PostgreSQL 16**, **MongoDB 7**, **Docker Compose**, and **Bash**.  
The pipeline ingests heterogeneous raw datasets (**CSV**, **nested JSON**, **Parquet**), enforces schema & data-quality rules with quarantine routing, standardizes and masks PII (salted SHA-256 + display redaction), and performs idempotent dual-target loads into a normalized **3NF PostgreSQL warehouse** and **MongoDB document collections**.

---

## Architecture Overview

```text
+-------------------------------------------------------------------------+
|                    1. SYNTHETIC / RAW MULTI-SOURCE DATA                 |
|  customers.csv (10,000) | orders.json (25,000) | activity_logs.parquet  |
|                         |   [nested items/addr]|       (20,000)         |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                     2. EXTRACT (src/extract.py)                         |
|  - Modular CSV, JSON (address flattening + nested payload preservation),|
|    and PyArrow Parquet readers with ingestion metadata                  |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|              3. DATA QUALITY & QUARANTINE (src/validate.py)             |
|  - Required schema, non-nulls, regex (email/phone/IPv4), bounds, FK     |
|  - Invalid rows -> data/quarantine/<source>_quarantine_<ts>.json        |
|  - Summary metrics -> logs/dq_report_<ts>.json                          |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|          4. TRANSFORM, DEDUPE & PII MASKING (src/transform.py)          |
|  - PK deduplication keeping latest UTC timestamp                        |
|  - UTC ISO-8601 normalization, string trimming/casing, null imputation  |
|  - Salted SHA-256 hashing (email_hash, phone_hash, ip_hash)             |
|  - Display masking (j***@domain.com, XXXXXX7490, 192.168.xxx.xxx)       |
|  - Raw street address stripping & order-item 3NF normalization          |
+-----------------+---------------------------------------+---------------+
                  |                                       |
                  v                                       v
+-----------------------------------+   +---------------------------------+
| 5A. POSTGRESQL 16 (3NF Warehouse) |   | 5B. MONGODB 7 (Event & Audit)   |
|  - dim_customers                  |   |  - activity_logs (compound idx) |
|  - fact_orders                    |   |  - etl_audit_reports            |
|  - fact_order_items               |   |  - Bulk UpdateOne(..., upsert)  |
|  - etl_batch_runs                 |   +---------------------------------+
|  - INSERT ... ON CONFLICT DO UPDATE
+-----------------------------------+
                  |
                  v
+-------------------------------------------------------------------------+
|          6. BASH ORCHESTRATION & ARCHIVAL (scripts/run_etl.sh)          |
|  - set -euo pipefail, ERR/EXIT traps, DB readiness checks               |
|  - Timestamped logs -> logs/ | Raw archival -> data/processed/          |
+-------------------------------------------------------------------------+
```

---

## Repository Structure

```text
etl-data-pipeline/
├── config/
│   └── settings.py              # Env config, file paths, schema rules, thresholds
├── data/
│   ├── raw/                     # Generated raw CSV, JSON, and Parquet files
│   ├── quarantine/              # Rejected/invalid records with error metadata
│   └── processed/               # Archived processed files
├── logs/                        # Timestamped Bash & Python execution logs + DQ reports
├── sql/
│   └── init_postgres.sql        # Normalized 3NF DDL (PKs, FKs, CHECKs, Indexes)
├── src/
│   ├── generate_sample_data.py  # Generates 55,000 realistic messy records
│   ├── extract.py               # Modular readers for CSV, nested JSON, and Parquet
│   ├── validate.py              # Schema validation, range/regex checks, quarantine routing
│   ├── transform.py             # Deduplication, standardization, PII masking, enrichment
│   ├── load_postgres.py         # Idempotent batch upserts into PostgreSQL
│   ├── load_mongodb.py          # Bulk UpdateOne upserts & compound indexes in MongoDB
│   └── pipeline.py              # Main CLI entrypoint (Extract -> Validate -> Transform -> Load)
├── scripts/
│   ├── run_etl.sh               # Master Bash runner with logging, traps, and archival
│   └── schedule_cron.sh         # Helper script for daily crontab batch scheduling
├── tests/
│   ├── conftest.py
│   ├── test_validation.py       # Schema enforcement, quarantine routing, DQ report tests
│   └── test_transformations.py  # Deduplication, imputation, and PII masking tests
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quickstart

### 1. Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Bash (`bash` on Linux/macOS or Git Bash on Windows)

### 2. Install Dependencies & Configure Environment
```bash
pip install -r requirements.txt
cp .env.example .env
```

### 3. Start PostgreSQL 16 & MongoDB 7
```bash
docker compose up -d --wait
```
> **Note:** By default, `.env.example` and `docker-compose.yml` bind PostgreSQL to `5433` and MongoDB to `27018` on localhost to avoid collisions with pre-installed local database services. Override `POSTGRES_PORT` / `MONGO_PORT` in `.env` if needed.

### 4. Run the End-to-End Pipeline
```bash
./scripts/run_etl.sh
```
On the first run (when `data/raw/` is empty), `run_etl.sh` automatically triggers `src/generate_sample_data.py` via `--generate-if-missing`, validates and transforms all 55,000 records, loads both databases idempotently, writes the Data Quality Report to `logs/`, and archives processed raw files to `data/processed/`.

To run a dry-run without database containers:
```bash
python -m src.pipeline --generate-if-missing --skip-db-load
```

### 5. Run the Unit Test Suite
```bash
python -m pytest -v
```

### 6. Cron Scheduling (Optional)
```bash
./scripts/schedule_cron.sh           # Preview daily 02:00 UTC crontab entry
./scripts/schedule_cron.sh --install # Register in user crontab
```

---

## Verified Benchmark Results

Measured locally end-to-end (including synthetic data generation, extraction, validation, quarantine JSON export, transformation, PII masking, and dual-database batch upserts into Dockerized PostgreSQL 16 & MongoDB 7):

| Metric | Observed Value |
| --- | --- |
| **Total Records Generated & Read** | **55,000** (`10,000` CSV + `25,000` JSON + `20,000` Parquet) |
| **Valid Records Passed** | **51,739** (`94.07%` pass rate) |
| **Quarantined Records** | **3,261** (`5.93%` quarantine rate) |
| **Duplicates Removed (Transform)** | **2,068** (`376` customers + `936` orders + `756` events) |
| **PostgreSQL `dim_customers`** | **9,094** rows |
| **PostgreSQL `fact_orders`** | **22,475** rows |
| **PostgreSQL `fact_order_items`** | **44,888** rows |
| **PostgreSQL `etl_batch_runs`** | **1** audit row |
| **MongoDB `activity_logs`** | **18,102** documents |
| **MongoDB `etl_audit_reports`** | **1** audit document |
| **Stage Timings** | Extract `0.351s` \| Validate `3.074s` \| Transform `0.901s` \| Postgres `7.502s` \| Mongo `2.244s` |
| **Total End-to-End Runtime** | **15.721s** |

---

## Sample Data Quality Report (`logs/dq_report_<timestamp>.json`)

```json
{
  "batch_id": "batch_20261125_171552",
  "generated_at": "2026-11-25T17:15:58.031032+00:00",
  "summary": {
    "total_rows_read": 55000,
    "total_valid_rows": 51739,
    "total_quarantined_rows": 3261,
    "overall_pass_rate_pct": 94.07,
    "quarantine_rate_pct": 5.93,
    "total_exact_duplicates_detected": 1111,
    "total_pk_duplicates_detected": 2173
  },
  "sources": {
    "customers": {
      "rows_read": 10000,
      "valid_rows": 9470,
      "quarantined_rows": 530,
      "pass_rate_pct": 94.7,
      "exact_duplicates": 204,
      "pk_duplicates": 391,
      "duplicates_removed_in_transform": 376,
      "rejection_breakdown": {
        "null_customer_id": 145,
        "invalid_email_format": 84,
        "invalid_kyc_status": 77,
        "invalid_signup_date": 58,
        "invalid_phone_format": 58,
        "null_phone": 44,
        "null_email": 34,
        "null_signup_date": 30
      }
    },
    "orders": {
      "rows_read": 25000,
      "valid_rows": 23411,
      "quarantined_rows": 1589,
      "pass_rate_pct": 93.64,
      "exact_duplicates": 504,
      "pk_duplicates": 988,
      "duplicates_removed_in_transform": 936
    },
    "activity_logs": {
      "rows_read": 20000,
      "valid_rows": 18858,
      "quarantined_rows": 1142,
      "pass_rate_pct": 94.29,
      "exact_duplicates": 403,
      "pk_duplicates": 794,
      "duplicates_removed_in_transform": 756
    }
  }
}
```
