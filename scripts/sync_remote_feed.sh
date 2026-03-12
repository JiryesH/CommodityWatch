#!/usr/bin/env bash
set -euo pipefail

branch="${1:-feed-data}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${root}"
git fetch origin "${branch}"
mkdir -p data
git show "origin/${branch}:data/feed.json" > data/feed.local.json
printf 'Updated data/feed.local.json from origin/%s\n' "${branch}"
