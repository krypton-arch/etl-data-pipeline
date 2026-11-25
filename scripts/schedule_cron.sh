#!/usr/bin/env bash
set -euo pipefail

# Helper script demonstrating production cron scheduling for the ETL pipeline.
# Usage:
#   ./scripts/schedule_cron.sh             # Preview cron entry (dry-run)
#   ./scripts/schedule_cron.sh --install   # Register daily 02:00 UTC job in crontab
#   ./scripts/schedule_cron.sh --remove    # Remove ETL job from crontab

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER_PATH="${PROJECT_ROOT}/scripts/run_etl.sh"
CRON_SCHEDULE="${CRON_SCHEDULE:-0 2 * * *}"
CRON_TAG="# multi-source-etl-pipeline"
CRON_CMD="${CRON_SCHEDULE} cd ${PROJECT_ROOT} && ${RUNNER_PATH} >> ${PROJECT_ROOT}/logs/cron.log 2>&1 ${CRON_TAG}"

ACTION="${1:---dry-run}"

case "${ACTION}" in
    --dry-run)
        echo "[DRY-RUN] Suggested crontab entry (daily at 02:00):"
        echo "${CRON_CMD}"
        echo ""
        echo "Run './scripts/schedule_cron.sh --install' to register this job."
        ;;
    --install)
        if ! command -v crontab >/dev/null 2>&1; then
            echo "[ERROR] 'crontab' command is not available on this host." >&2
            exit 1
        fi
        existing_cron="$(crontab -l 2>/dev/null || true)"
        filtered_cron="$(printf "%s\n" "${existing_cron}" | grep -v "${CRON_TAG}" || true)"
        {
            printf "%s\n" "${filtered_cron}"
            printf "%s\n" "${CRON_CMD}"
        } | sed '/^$/d' | crontab -
        echo "[INFO] Registered cron job:"
        crontab -l | grep "${CRON_TAG}"
        ;;
    --remove)
        if ! command -v crontab >/dev/null 2>&1; then
            echo "[ERROR] 'crontab' command is not available on this host." >&2
            exit 1
        fi
        existing_cron="$(crontab -l 2>/dev/null || true)"
        printf "%s\n" "${existing_cron}" | grep -v "${CRON_TAG}" | crontab -
        echo "[INFO] Removed ETL cron job from crontab."
        ;;
    *)
        echo "Usage: $0 [--dry-run|--install|--remove]" >&2
        exit 2
        ;;
esac
