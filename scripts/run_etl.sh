#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

TIMESTAMP="$(date -u +%Y%m%d_%H%M%S)"
LOGS_DIR="${PROJECT_ROOT}/logs"
RAW_DIR="${PROJECT_ROOT}/data/raw"
PROCESSED_DIR="${PROJECT_ROOT}/data/processed"
QUARANTINE_DIR="${PROJECT_ROOT}/data/quarantine"

mkdir -p "${LOGS_DIR}" "${RAW_DIR}" "${PROCESSED_DIR}" "${QUARANTINE_DIR}"
LOG_FILE="${LOGS_DIR}/etl_run_${TIMESTAMP}.log"

exec > >(tee -a "${LOG_FILE}") 2>&1

cleanup_on_exit() {
    local exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [INFO] ETL orchestration finished successfully (exit_code=0)."
    else
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [ERROR] ETL orchestration failed (exit_code=${exit_code}). See ${LOG_FILE}." >&2
    fi
}
trap cleanup_on_exit EXIT
trap 'echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [ERROR] Command failed at line ${LINENO}: ${BASH_COMMAND}" >&2' ERR

echo "=================================================================="
echo " Multi-Source ETL & Data Quality Pipeline - Batch Runner"
echo " Started at (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo " Log File        : ${LOG_FILE}"
echo "=================================================================="

# Load environment variables from .env (fallback to .env.example)
if [[ -f "${PROJECT_ROOT}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_ROOT}/.env"
    set +a
    echo "[INFO] Loaded environment from .env"
elif [[ -f "${PROJECT_ROOT}/.env.example" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_ROOT}/.env.example"
    set +a
    echo "[WARN] .env not found; loaded defaults from .env.example"
fi

# Resolve Python interpreter (prefer active venv or interpreter with ETL deps installed)
if [[ -n "${PYTHON_BIN:-}" ]]; then
    :
elif [[ -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
    PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
elif [[ -x "${PROJECT_ROOT}/.venv/Scripts/python.exe" ]]; then
    PYTHON_BIN="${PROJECT_ROOT}/.venv/Scripts/python.exe"
else
    PYTHON_BIN=""
    for candidate in python python3; do
        if command -v "${candidate}" >/dev/null 2>&1; then
            if "${candidate}" -c "import pandas, psycopg2, pymongo" >/dev/null 2>&1; then
                PYTHON_BIN="${candidate}"
                break
            elif [[ -z "${PYTHON_BIN}" ]]; then
                PYTHON_BIN="${candidate}"
            fi
        fi
    done
fi

if [[ -z "${PYTHON_BIN}" ]]; then
    echo "[ERROR] Python interpreter not found in PATH." >&2
    exit 127
fi

echo "[INFO] Using Python: $(${PYTHON_BIN} --version 2>&1)"

# Check if --skip-db-load is present
SKIP_DB_LOAD=false
for arg in "$@"; do
    if [[ "${arg}" == "--skip-db-load" ]]; then
        SKIP_DB_LOAD=true
    fi
done

if [[ "${SKIP_DB_LOAD}" == "false" ]]; then
    echo "[INFO] Verifying PostgreSQL and MongoDB readiness..."
    "${PYTHON_BIN}" - <<'PY'
import os
import sys
import time
import psycopg2
from pymongo import MongoClient

pg_dsn = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'etl_user')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'etl_password')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5433')}/"
    f"{os.getenv('POSTGRES_DB', 'etl_warehouse')}"
)
mongo_uri = os.getenv(
    "MONGO_URI",
    "mongodb://etl_user:etl_password@localhost:27018/etl_events?authSource=admin",
)

max_attempts = 15
for attempt in range(1, max_attempts + 1):
    try:
        conn = psycopg2.connect(pg_dsn, connect_timeout=3)
        conn.close()
        print(f"[INFO] PostgreSQL is ready (attempt {attempt}).")
        break
    except Exception as exc:
        if attempt == max_attempts:
            print(f"[ERROR] PostgreSQL readiness check failed: {exc}", file=sys.stderr)
            sys.exit(1)
        time.sleep(2)

for attempt in range(1, max_attempts + 1):
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        client.close()
        print(f"[INFO] MongoDB is ready (attempt {attempt}).")
        break
    except Exception as exc:
        if attempt == max_attempts:
            print(f"[ERROR] MongoDB readiness check failed: {exc}", file=sys.stderr)
            sys.exit(1)
        time.sleep(2)
PY
else
    echo "[INFO] Skipping database readiness checks (--skip-db-load)."
fi

echo "[INFO] Executing ETL pipeline..."
"${PYTHON_BIN}" -m src.pipeline --generate-if-missing "$@"

# Archive processed raw files with run timestamp
echo "[INFO] Archiving processed raw files to ${PROCESSED_DIR}..."
for raw_file in "${RAW_DIR}/customers.csv" "${RAW_DIR}/orders.json" "${RAW_DIR}/activity_logs.parquet"; do
    if [[ -f "${raw_file}" ]]; then
        base_name="$(basename "${raw_file}")"
        stem="${base_name%.*}"
        ext="${base_name##*.}"
        archived_path="${PROCESSED_DIR}/${stem}_${TIMESTAMP}.${ext}"
        mv "${raw_file}" "${archived_path}"
        echo "[INFO] Archived ${base_name} -> ${archived_path}"
    fi
done

echo "[INFO] Pipeline run and archival completed cleanly."
