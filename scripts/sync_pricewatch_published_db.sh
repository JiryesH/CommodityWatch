#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_db="${root}/data/commodities.db"

if [[ -n "${COMMODITY_BACKEND_ROOT:-}" ]]; then
  source_root="$(cd "${COMMODITY_BACKEND_ROOT}" && pwd)"
else
  source_root="$(cd "${root}/../Commodity Prices" && pwd)"
fi

source_db="${source_root}/data/commodities.db"

if [[ ! -f "${source_db}" ]]; then
  printf 'Commodity source database not found: %s\n' "${source_db}" >&2
  exit 1
fi

if [[ ! -s "${source_db}" ]]; then
  printf 'Commodity source database is empty: %s\n' "${source_db}" >&2
  exit 1
fi

mkdir -p "$(dirname "${target_db}")"
tmp_db="${target_db}.tmp"
cp "${source_db}" "${tmp_db}"
mv "${tmp_db}" "${target_db}"

printf 'Synced PriceWatch published database to %s from %s\n' "${target_db}" "${source_db}"
