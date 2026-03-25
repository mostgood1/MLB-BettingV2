#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${MLB_BETTING_BASE_URL:-${BASE_URL:-${RENDER_URL:-${RENDER_EXTERNAL_URL:-}}}}"
TOKEN="${MLB_BETTING_CRON_TOKEN:-${CRON_TOKEN:-}}"
PATH_QS="${1:-/api/cron/ping}"

if [[ -z "${BASE_URL}" ]]; then
  echo "Missing base URL. Set MLB_BETTING_BASE_URL, RENDER_URL, or BASE_URL." >&2
  exit 2
fi
if [[ -z "${TOKEN}" ]]; then
  echo "Missing cron token. Set MLB_BETTING_CRON_TOKEN or CRON_TOKEN." >&2
  exit 2
fi

URL="${BASE_URL%/}${PATH_QS}"
if [[ "${PATH_QS}" != /* ]]; then
  URL="${BASE_URL%/}/${PATH_QS}"
fi

echo "[render-cron] GET ${URL}" >&2

max_attempts="${MLB_BETTING_CRON_MAX_ATTEMPTS:-6}"
sleep_base_seconds="${MLB_BETTING_CRON_SLEEP_BASE_SECONDS:-5}"

attempt=1
while true; do
  http=""
  curl_rc=0

  set +e
  http=$(curl -sS -o resp.txt -w "%{http_code}" -H "Authorization: Bearer ${TOKEN}" "${URL}")
  curl_rc=$?
  set -e

  if [[ ${curl_rc} -ne 0 ]]; then
    http="000"
  fi

  if [[ "${http}" =~ ^2[0-9][0-9]$ ]]; then
    cat resp.txt
    exit 0
  fi

  echo "[render-cron] attempt ${attempt}/${max_attempts} -> HTTP ${http}" >&2
  cat resp.txt >&2 || true

  if [[ ${attempt} -ge ${max_attempts} ]]; then
    exit 1
  fi

  if [[ "${http}" == "000" || "${http}" == "429" || "${http}" =~ ^5[0-9][0-9]$ ]]; then
    sleep_seconds=$(( attempt * sleep_base_seconds ))
    echo "[render-cron] retrying in ${sleep_seconds}s" >&2
    sleep "${sleep_seconds}"
    attempt=$((attempt + 1))
    continue
  fi

  exit 1
done