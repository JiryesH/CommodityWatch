#!/usr/bin/env bash
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
backend_dir="${root}/backend"
backend_python="${backend_dir}/.venv/bin/python"
backend_env="${backend_dir}/.env"
publish_script="${root}/scripts/publish_inventorywatch_store.py"
audit_script="${root}/scripts/audit_inventorywatch_coverage.py"
usda_backfill_script="${root}/scripts/backfill/usda_wasde_historical.py"
lme_backfill_script="${root}/scripts/backfill/lme_historical.py"
seed_script="${backend_dir}/scripts/seed_reference_data.py"
audit_json="${backend_dir}/artifacts/inventorywatch/published_coverage_audit.json"
audit_markdown="${backend_dir}/artifacts/inventorywatch/published_coverage_audit.md"

declare -a preflight_failures=()
declare -a source_failures=()
declare -a publish_failures=()
declare -a coverage_failures=()
declare -a warnings=()
refresh_mode="${CW_INVENTORYWATCH_REFRESH_MODE:-operator}"

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --strict)
      refresh_mode="strict"
      shift
      ;;
    --operator)
      refresh_mode="operator"
      shift
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

requested_jobs=("$@")

require_file() {
  local path="$1"
  local message="$2"
  if [[ ! -e "${path}" ]]; then
    printf '%s\n' "${message}" >&2
    exit 1
  fi
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

append_preflight_failure() {
  local item="$1"
  preflight_failures+=("${item}")
}

append_source_failure() {
  local item="$1"
  source_failures+=("${item}")
}

append_publish_failure() {
  local item="$1"
  publish_failures+=("${item}")
}

append_coverage_failure() {
  local item="$1"
  coverage_failures+=("${item}")
}

append_warning() {
  local item="$1"
  warnings+=("${item}")
}

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

require_file "${backend_python}" "Missing ${backend_python}. Set up the backend first in backend/."
require_file "${backend_env}" "Missing backend/.env. Copy backend/.env.example to backend/.env first."
require_file "${seed_script}" "Missing ${seed_script}. Cannot seed backend reference data."
require_file "${publish_script}" "Missing ${publish_script}. Cannot publish InventoryWatch data."
require_file "${audit_script}" "Missing ${audit_script}. Cannot audit InventoryWatch coverage."

set -a
source "${backend_env}"
set +a

run_backend_task() {
  local classification="$1"
  local name="$2"
  shift 2
  log "Running InventoryWatch task: ${name}"
  (
    cd "${backend_dir}"
    PYTHONUNBUFFERED=1 "${backend_python}" "$@"
  )
  local status=$?
  if [[ "${status}" -eq 0 ]]; then
    log "Finished InventoryWatch task: ${name}"
    return 0
  fi

  case "${classification}" in
    preflight)
      append_preflight_failure "${name}"
      ;;
    source)
      append_source_failure "${name}"
      ;;
    publish)
      append_publish_failure "${name}"
      ;;
    coverage)
      append_coverage_failure "${name}"
      ;;
  esac
  log "FAILED InventoryWatch ${classification} task: ${name} (exit=${status})"
  return "${status}"
}

run_job() {
  local job="$1"
  run_backend_task "source" "${job}" -m app.worker run-once --job "${job}"
}

run_backfill_script() {
  local name="$1"
  shift
  run_backend_task "source" "${name}" "$@"
}

seed_reference_data() {
  run_backend_task "preflight" "seed_reference_data" "${seed_script}"
}

upgrade_database_schema() {
  run_backend_task "preflight" "alembic_upgrade_head" -m alembic upgrade head
}

validate_migration_revisions() {
  run_backend_task "preflight" "inventorywatch_migration_preflight" -m app.ingest.common.jobs validate-migrations
}

run_schema_preflight() {
  run_backend_task "preflight" "inventorywatch_schema_preflight" -m app.ingest.common.jobs check-schema
}

publish_local_store() {
  local mode="${1:-operator}"
  run_backend_task "publish" "publish_inventorywatch_store" "${publish_script}" \
    --data-root "${backend_dir}" \
    --output "${root}/data/inventorywatch.db" \
    --audit-json "${audit_json}" \
    --audit-markdown "${audit_markdown}" || return 1

  log "Evaluating InventoryWatch coverage audit (${mode})"
  (
    cd "${backend_dir}"
    PYTHONUNBUFFERED=1 "${backend_python}" "${audit_script}" \
      --database "${root}/data/inventorywatch.db" \
      --audit-json "${audit_json}" \
      --audit-markdown "${audit_markdown}" \
      --fail-on-weak-coverage
  )
  local status=$?
  if [[ "${status}" -eq 0 ]]; then
    log "Finished InventoryWatch coverage audit (${mode})"
    return 0
  fi

  append_coverage_failure "published_coverage_audit"
  if [[ "${mode}" == "strict" ]]; then
    log "FAILED InventoryWatch coverage gate: published_coverage_audit (exit=${status})"
    return "${status}"
  fi
  log "InventoryWatch coverage gate reported blocking issues; operator mode will continue."
  return 0
}

missing_key_for_job() {
  local job="$1"
  case "${job}" in
    eia_wpsr|eia_wngs)
      [[ -n "${CW_EIA_API_KEY:-}" ]] || { printf 'CW_EIA_API_KEY is not set in backend/.env'; return 0; }
      ;;
    agsi_daily)
      [[ -n "${CW_AGSI_API_KEY:-}" ]] || { printf 'CW_AGSI_API_KEY is not set in backend/.env'; return 0; }
      ;;
  esac
  return 1
}

job_enabled_in_default_refresh() {
  local job="$1"
  case "${job}" in
    lme_warehouse)
      is_truthy "${CW_ENABLE_LME_LIVE_JOBS:-false}"
      ;;
    ice_certified)
      is_truthy "${CW_ENABLE_ICE_CERTIFIED_JOBS:-false}"
      ;;
    *)
      return 0
      ;;
  esac
}

run_job_if_possible() {
  local job="$1"
  local mode="${2:-default}"
  local missing_key

  if [[ "${mode}" == "default" ]] && ! job_enabled_in_default_refresh "${job}"; then
    case "${job}" in
      lme_warehouse)
        log "Skipping lme_warehouse in default refresh because live public access is unreliable. Use scripts/backfill/lme_historical.py for archive loads or set CW_ENABLE_LME_LIVE_JOBS=true."
        append_warning "lme_warehouse_skipped"
        ;;
      ice_certified)
        log "Skipping ice_certified in default refresh because ICE is deferred for now. Set CW_ENABLE_ICE_CERTIFIED_JOBS=true to force it back on."
        append_warning "ice_certified_skipped"
        ;;
    esac
    return 0
  fi

  if missing_key="$(missing_key_for_job "${job}")"; then
    log "Skipping ${job} because ${missing_key}."
    append_warning "${job}_skipped_missing_key"
    return 0
  fi

  run_job "${job}" || return 0
}

run_historical_backfills() {
  local backfill_from_usda="${CW_USDA_BACKFILL_FROM:-2000-01-01}"
  local backfill_from_lme="${CW_LME_BACKFILL_FROM:-2010-01-01}"
  local today
  today="$(date +%F)"

  # Historical archive backfills are explicit maintenance operations, not the default operator refresh path.
  if is_truthy "${CW_ENABLE_USDA_HISTORICAL_BACKFILL:-false}"; then
    if [[ -e "${usda_backfill_script}" ]]; then
      run_backfill_script "usda_wasde_historical" "${usda_backfill_script}" --from "${backfill_from_usda}" --to "${today}" || true
    else
      log "Skipping usda_wasde_historical because ${usda_backfill_script} was not found."
      append_warning "usda_wasde_historical_missing"
    fi
  fi

  if is_truthy "${CW_ENABLE_LME_HISTORICAL_BACKFILL:-false}"; then
    if [[ -e "${lme_backfill_script}" ]]; then
      run_backfill_script "lme_historical" "${lme_backfill_script}" --from "${backfill_from_lme}" --to "${today}" || true
    else
      log "Skipping lme_historical because ${lme_backfill_script} was not found."
      append_warning "lme_historical_missing"
    fi
  fi
}

run_post_refresh_steps() {
  local run_seasonal="${1:-true}"
  local run_publish="${2:-true}"
  local mode="${3:-operator}"

  if is_truthy "${run_seasonal}"; then
    run_job "seasonal_ranges" || true
  fi
  if is_truthy "${run_publish}"; then
    publish_local_store "${mode}" || true
  fi
}

finish_with_summary() {
  local mode="${1:-operator}"
  if [[ "${#preflight_failures[@]}" -gt 0 ]]; then
    log "InventoryWatch refresh failed during preflight: ${preflight_failures[*]}"
    return 1
  fi
  if [[ "${#source_failures[@]}" -gt 0 ]]; then
    log "InventoryWatch refresh completed with source-job failures: ${source_failures[*]}"
    return 1
  fi
  if [[ "${#publish_failures[@]}" -gt 0 ]]; then
    log "InventoryWatch refresh completed with publish failures: ${publish_failures[*]}"
    return 1
  fi
  if [[ "${#coverage_failures[@]}" -gt 0 ]]; then
    if [[ "${mode}" == "strict" ]]; then
      log "InventoryWatch refresh failed strict coverage gate: ${coverage_failures[*]}"
      return 1
    fi
    log "InventoryWatch refresh completed with weak coverage in operator mode: ${coverage_failures[*]}"
  fi
  if [[ "${#warnings[@]}" -gt 0 ]]; then
    log "InventoryWatch refresh completed with warnings: ${warnings[*]}"
  fi
  log "InventoryWatch refresh complete."
  return 0
}

validate_migration_revisions || exit 1
upgrade_database_schema || exit 1
run_schema_preflight || exit 1
seed_reference_data || exit 1

if [[ "${#requested_jobs[@]}" -gt 0 ]]; then
  requested_seasonal=false
  requested_publish=false
  for job in "${requested_jobs[@]}"; do
    case "${job}" in
      backfill_usda_wasde)
        run_backfill_script "usda_wasde_historical" "${usda_backfill_script}" --from "${CW_USDA_BACKFILL_FROM:-2000-01-01}" --to "$(date +%F)" || true
        continue
        ;;
      backfill_lme_warehouse)
        run_backfill_script "lme_historical" "${lme_backfill_script}" --from "${CW_LME_BACKFILL_FROM:-2010-01-01}" --to "$(date +%F)" || true
        continue
        ;;
      eia_wpsr|eia_wngs|agsi_daily|usda_wasde|lme_warehouse|comex_warehouse|etf_holdings|ice_certified)
        ;;
      seasonal_ranges)
        run_job "seasonal_ranges" || true
        requested_seasonal=true
        continue
        ;;
      publish_local_store)
        publish_local_store "${refresh_mode}" || true
        requested_publish=true
        continue
        ;;
      seed_reference_data)
        seed_reference_data || exit 1
        continue
        ;;
      *)
        printf 'Unknown InventoryWatch job: %s\n' "${job}" >&2
        exit 1
        ;;
    esac
    run_job_if_possible "${job}" "explicit"
  done
  run_seasonal_after=true
  run_publish_after=true
  if [[ "${requested_publish}" == true ]]; then
    run_seasonal_after=false
    run_publish_after=false
  elif [[ "${requested_seasonal}" == true ]]; then
    run_seasonal_after=false
    run_publish_after=true
  fi
  run_post_refresh_steps "${run_seasonal_after}" "${run_publish_after}" "${refresh_mode}"
  finish_with_summary "${refresh_mode}"
  exit $?
fi

run_historical_backfills

for job in \
  eia_wpsr \
  eia_wngs \
  agsi_daily \
  usda_wasde \
  comex_warehouse \
  etf_holdings \
  lme_warehouse \
  ice_certified
do
  run_job_if_possible "${job}" "default"
done

run_post_refresh_steps true true "${refresh_mode}"
finish_with_summary "${refresh_mode}"
exit $?
