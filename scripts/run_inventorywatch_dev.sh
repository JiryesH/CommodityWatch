#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
backend_dir="${root}/backend"
frontend_dir="${root}/frontend"

backend_host="${INVENTORYWATCH_BACKEND_HOST:-127.0.0.1}"
backend_port="${INVENTORYWATCH_BACKEND_PORT:-8000}"
frontend_host="${INVENTORYWATCH_FRONTEND_HOST:-127.0.0.1}"
frontend_port="${INVENTORYWATCH_FRONTEND_PORT:-3000}"
api_base_url="${NEXT_PUBLIC_API_BASE_URL:-http://${backend_host}:${backend_port}/api}"

backend_python="${backend_dir}/.venv/bin/python"
backend_uvicorn="${backend_dir}/.venv/bin/uvicorn"
frontend_node_modules="${frontend_dir}/node_modules"

backend_started_by_script=0
backend_pid=""

cleanup() {
  if [[ "${backend_started_by_script}" -eq 1 && -n "${backend_pid}" ]]; then
    kill "${backend_pid}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

listening_pid() {
  local port="$1"
  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi
  lsof -t -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
}

require_file() {
  local path="$1"
  local message="$2"
  if [[ ! -e "${path}" ]]; then
    printf '%s\n' "${message}" >&2
    exit 1
  fi
}

require_file "${backend_python}" "Missing ${backend_python}. Set up the backend first in backend/."
require_file "${backend_uvicorn}" "Missing ${backend_uvicorn}. Set up the backend first in backend/."
require_file "${frontend_dir}/package.json" "Missing frontend/package.json."
require_file "${backend_dir}/.env" "Missing backend/.env. Copy backend/.env.example to backend/.env first."

if [[ ! -d "${frontend_node_modules}" ]]; then
  printf 'Missing frontend/node_modules. Run: cd %s && npm install\n' "${frontend_dir}" >&2
  exit 1
fi

if curl -fsS "http://${backend_host}:${backend_port}/api/health/live" >/dev/null 2>&1; then
  printf 'InventoryWatch backend already running at http://%s:%s\n' "${backend_host}" "${backend_port}"
else
  existing_backend_pid="$(listening_pid "${backend_port}")"
  if [[ -n "${existing_backend_pid}" ]]; then
    printf 'Port %s is already in use by PID %s, but the backend health check failed.\n' "${backend_port}" "${existing_backend_pid}" >&2
    printf 'Stop the stale backend process or change INVENTORYWATCH_BACKEND_PORT before retrying.\n' >&2
    exit 1
  fi

  printf 'Starting InventoryWatch backend at http://%s:%s\n' "${backend_host}" "${backend_port}"
  (
    cd "${backend_dir}"
    exec "${backend_uvicorn}" app.main:app --reload --host "${backend_host}" --port "${backend_port}"
  ) &
  backend_pid="$!"
  backend_started_by_script=1

  for _ in {1..30}; do
    if curl -fsS "http://${backend_host}:${backend_port}/api/health/live" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if ! curl -fsS "http://${backend_host}:${backend_port}/api/health/live" >/dev/null 2>&1; then
    printf 'Backend did not become ready.\n' >&2
    printf 'Check backend/.env, PostgreSQL, and migrations.\n' >&2
    printf '\nIf PostgreSQL is not installed on this Mac, the quickest local path is:\n' >&2
    printf '  brew install postgresql@16\n' >&2
    printf '  brew services start postgresql@16\n' >&2
    printf '  $(brew --prefix postgresql@16)/bin/createdb commoditywatch\n' >&2
    printf '\nThen set CW_DATABASE_URL in backend/.env to something like:\n' >&2
    printf '  postgresql+asyncpg://%s@localhost:5432/commoditywatch\n' "${USER}" >&2
    exit 1
  fi
fi

if curl -fsS "http://${frontend_host}:${frontend_port}/inventory" >/dev/null 2>&1; then
  printf 'InventoryWatch frontend already running at http://%s:%s\n' "${frontend_host}" "${frontend_port}"
  printf 'Inventory URL: http://%s:%s/inventory\n' "${frontend_host}" "${frontend_port}"
  printf 'Using API base: %s\n' "${api_base_url}"
  exit 0
fi

existing_frontend_pid="$(listening_pid "${frontend_port}")"
if [[ -n "${existing_frontend_pid}" ]]; then
  printf 'Port %s is already in use by PID %s, but the frontend did not respond successfully.\n' "${frontend_port}" "${existing_frontend_pid}" >&2
  printf 'Stop the stale frontend process or change INVENTORYWATCH_FRONTEND_PORT before retrying.\n' >&2
  exit 1
fi

printf 'Starting InventoryWatch frontend at http://%s:%s\n' "${frontend_host}" "${frontend_port}"
printf 'Inventory URL: http://%s:%s/inventory\n' "${frontend_host}" "${frontend_port}"
printf 'Using API base: %s\n' "${api_base_url}"

cd "${frontend_dir}"
NEXT_PUBLIC_API_BASE_URL="${api_base_url}" npm run dev -- --hostname "${frontend_host}" --port "${frontend_port}"
