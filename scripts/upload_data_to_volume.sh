#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-DEFAULT}"
CATALOG="${2:-dev}"
SCHEMA="${3:-bronze}"
LOCAL_DATA_DIR="${4:-data}"
TARGET="dbfs:/Volumes/${CATALOG}/${SCHEMA}/raw/data"

if [[ ! -d "${LOCAL_DATA_DIR}" ]]; then
  echo "Local data directory not found: ${LOCAL_DATA_DIR}" >&2
  exit 1
fi

echo "Uploading ${LOCAL_DATA_DIR} -> ${TARGET} (profile=${PROFILE})"
databricks fs cp --recursive "${LOCAL_DATA_DIR}" "${TARGET}" --profile "${PROFILE}"

echo "Upload complete."
