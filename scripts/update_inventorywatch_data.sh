#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
backend_dir="${root}/backend"
backend_python="${backend_dir}/.venv/bin/python"
backend_env="${backend_dir}/.env"
publish_script="${root}/scripts/publish_inventorywatch_store.py"

require_file() {
  local path="$1"
  local message="$2"
  if [[ ! -e "${path}" ]]; then
    printf '%s\n' "${message}" >&2
    exit 1
  fi
}

require_file "${backend_python}" "Missing ${backend_python}. Set up the backend first in backend/."
require_file "${backend_env}" "Missing backend/.env. Copy backend/.env.example to backend/.env first."

set -a
source "${backend_env}"
set +a

run_job() {
  local job="$1"
  printf '[%s] Running InventoryWatch job: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "${job}"
  (
    cd "${backend_dir}"
    PYTHONUNBUFFERED=1 "${backend_python}" -m app.worker run-once --job "${job}"
  )
  printf '[%s] Finished InventoryWatch job: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "${job}"
}

publish_local_store() {
  printf '[%s] Publishing InventoryWatch local store\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  "${backend_python}" "${publish_script}" --data-root "${backend_dir}" --output "${root}/data/inventorywatch.db"
  printf '[%s] Published InventoryWatch local store\n' "$(date '+%Y-%m-%d %H:%M:%S')"
}

require_eia_key() {
  if [[ -z "${CW_EIA_API_KEY:-}" ]]; then
    printf 'CW_EIA_API_KEY is not set in backend/.env. EIA jobs cannot run.\n' >&2
    exit 1
  fi
}

require_agsi_key() {
  if [[ -z "${CW_AGSI_API_KEY:-}" ]]; then
    printf 'CW_AGSI_API_KEY is not set in backend/.env. AGSI jobs cannot run.\n' >&2
    exit 1
  fi
}

if [[ "$#" -gt 0 ]]; then
  for job in "$@"; do
    case "${job}" in
      eia_wpsr|eia_wngs)
        require_eia_key
        ;;
      agsi_daily)
        require_agsi_key
        ;;
    esac
    run_job "${job}"
  done
  publish_local_store
  exit 0
fi

require_eia_key
run_job "eia_wpsr"
run_job "eia_wngs"

if [[ -n "${CW_AGSI_API_KEY:-}" ]]; then
  run_job "agsi_daily"
else
  printf 'Skipping agsi_daily because CW_AGSI_API_KEY is not set in backend/.env\n'
fi

run_job "seasonal_ranges"
publish_local_store

printf 'InventoryWatch refresh complete.\n'
