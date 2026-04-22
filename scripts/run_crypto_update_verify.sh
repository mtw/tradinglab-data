#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_DIR"

resolve_venv_path() {
  if [[ -n "${TL_VENV_PATH:-}" ]]; then
    printf '%s\n' "$TL_VENV_PATH"
    return 0
  fi
  if [[ -n "${VIRTUAL_ENV:-}" ]]; then
    printf '%s\n' "$VIRTUAL_ENV"
    return 0
  fi
  if [[ -f "$REPO_DIR/.venv/bin/activate" ]]; then
    printf '%s\n' "$REPO_DIR/.venv"
    return 0
  fi
  return 1
}

resolve_config_path() {
  if [[ -n "${TLD_CONFIG_PATH:-}" ]]; then
    printf '%s\n' "$TLD_CONFIG_PATH"
    return 0
  fi
  if [[ -f "$REPO_DIR/configs/config.local.yaml" ]]; then
    printf '%s\n' "configs/config.local.yaml"
    return 0
  fi
  printf '%s\n' "configs/config.yaml"
}

VENV_PATH="$(resolve_venv_path || true)"
CONFIG_PATH="$(resolve_config_path)"
CRYPTO_REFRESH_UNIVERSE="${TLD_CRYPTO_REFRESH_UNIVERSE:-1}"
CRYPTO_REFRESH_PROVIDER="${TLD_CRYPTO_REFRESH_PROVIDER:-coingecko}"
CRYPTO_REFRESH_UNIVERSE_NAME="${TLD_CRYPTO_REFRESH_UNIVERSE_NAME:-crypto_high_liquidity}"
CRYPTO_REFRESH_LIMIT="${TLD_CRYPTO_REFRESH_LIMIT:-25}"
CRYPTO_UPDATE="${TLD_CRYPTO_UPDATE:-1}"
CRYPTO_INTERVALS="${TLD_CRYPTO_INTERVALS:-1d,1h,15m}"
CRYPTO_UNIVERSE="${TLD_CRYPTO_UNIVERSE:-crypto_high_liquidity}"
CRYPTO_REPAIR="${TLD_CRYPTO_REPAIR:-1}"
CRYPTO_MAX_MISSING_RATIO="${TLD_CRYPTO_MAX_MISSING_RATIO:-0.0}"
CRYPTO_MAX_ZERO_BYTE="${TLD_CRYPTO_MAX_ZERO_BYTE:-0}"
CRYPTO_STALE_MULTIPLE="${TLD_CRYPTO_STALE_MULTIPLE:-2}"
CRYPTO_PREFLIGHT_CHECK="${TLD_CRYPTO_PREFLIGHT_CHECK:-1}"
VERBOSE="${TL_VERBOSE:-1}"
DATE_KEY="${TL_GATE_DATE:-$(date +%F)}"
TS_KEY="$(date +%Y%m%dT%H%M%S)"
CURRENT_STAGE="startup"
FINALIZED=0

log() { echo "[$(date '+%F %T')] [$1] ${*:2}"; }

cleanup_lock() {
  if [[ -n "${LOCK_PATH:-}" ]]; then
    rmdir "$LOCK_PATH" 2>/dev/null || true
  fi
}

mark_fail() {
  local message="$1"
  rm -f "$RUNNING_FILE"
  printf '%s\n' "$message" > "$FAIL_FILE"
  FINALIZED=1
}

on_exit() {
  local rc="$?"
  cleanup_lock
  if [[ "$FINALIZED" -eq 1 ]]; then
    return
  fi
  if [[ "$rc" -ne 0 ]]; then
    mark_fail "unexpected_failure rc=${rc} stage=${CURRENT_STAGE}"
  else
    rm -f "$RUNNING_FILE"
  fi
}

run_cmd() {
  local label="$1"
  shift
  local start_ts end_ts rc elapsed
  start_ts="$(date +%s)"
  log INFO "RUN[$label]: $*"
  set +e
  "$@"
  rc=$?
  set -e
  end_ts="$(date +%s)"
  elapsed="$((end_ts - start_ts))"
  log INFO "DONE[$label]: rc=${rc} elapsed=${elapsed}s"
  return "$rc"
}

trap on_exit EXIT

if [[ "$VERBOSE" =~ ^[0-9]+$ ]] && [[ "$VERBOSE" -ge 2 ]]; then
  set -x
fi

if [[ -z "$VENV_PATH" || ! -f "$VENV_PATH/bin/activate" ]]; then
  echo "ERROR: no usable venv found; set TL_VENV_PATH, activate a venv first, or create $REPO_DIR/.venv" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$VENV_PATH/bin/activate"

cfg_path() {
  "$VENV_PATH/bin/python" - "$CONFIG_PATH" "$1" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, str((Path(sys.argv[1]).resolve().parent.parent / 'src')))
from tradinglab_data.config import Config, crypto_root_path, registry_root_path, runs_root_path  # noqa: E402
cfg = Config.load(sys.argv[1])
name = sys.argv[2]
mapping = {
  'crypto_root': crypto_root_path,
  'runs_root': runs_root_path,
  'registry_root': registry_root_path,
}
print(mapping[name](cfg))
PY
}

CRYPTO_ROOT="${TLD_CRYPTO_ROOT:-$(cfg_path crypto_root)}"
RUNS_ROOT="${TLD_RUNS_ROOT:-$(cfg_path runs_root)}"
REGISTRY_ROOT="${TLD_REGISTRY_ROOT:-$(cfg_path registry_root)}"
LOG_DIR="${TLD_LOG_DIR:-${REGISTRY_ROOT}/logs}"
GATE_DIR="${TLD_GATE_DIR:-${REGISTRY_ROOT}/crypto_update_gate}"
LOCK_DIR="${REGISTRY_ROOT}/locks"
LOCK_PATH="${LOCK_DIR}/crypto_update_verify.lockdir"
SUMMARY_DIR="${GATE_DIR}/summaries"
LOG_PATH="${LOG_DIR}/crypto_update_verify_${TS_KEY}.log"
OK_FILE="${GATE_DIR}/${DATE_KEY}.ok"
FAIL_FILE="${GATE_DIR}/${DATE_KEY}.fail"
RUNNING_FILE="${GATE_DIR}/${DATE_KEY}.running"

mkdir -p "$LOG_DIR" "$GATE_DIR" "$SUMMARY_DIR" "$LOCK_DIR"
exec >> "$LOG_PATH" 2>&1

if ! mkdir "$LOCK_PATH" 2>/dev/null; then
  log ERROR "another crypto update/verify run already holds lock: $LOCK_PATH"
  exit 4
fi
log INFO "lock acquired: $LOCK_PATH"
log INFO "using config: $CONFIG_PATH"

rm -f "$OK_FILE" "$FAIL_FILE" "$RUNNING_FILE"
{
  echo "running"
  echo "timestamp=$TS_KEY"
  echo "log=$LOG_PATH"
} > "$RUNNING_FILE"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"

if [[ "${CRYPTO_REFRESH_UNIVERSE}" == "1" ]]; then
  CURRENT_STAGE="crypto_refresh_universe"
  if run_cmd "crypto_refresh_universe" tradinglab-data --config "$CONFIG_PATH" crypto refresh-universe --provider "$CRYPTO_REFRESH_PROVIDER" --universe "$CRYPTO_REFRESH_UNIVERSE_NAME" --limit "$CRYPTO_REFRESH_LIMIT"; then
    :
  else
    rc=$?
    mark_fail "crypto_refresh_universe_failed rc=${rc} provider=${CRYPTO_REFRESH_PROVIDER} universe=${CRYPTO_REFRESH_UNIVERSE_NAME}"
    exit "$rc"
  fi
fi

IFS=',' read -r -a _TLD_CRYPTO_INTERVALS <<< "$CRYPTO_INTERVALS"
for interval in "${_TLD_CRYPTO_INTERVALS[@]}"; do
  interval="$(echo "$interval" | xargs)"
  [[ -z "$interval" ]] && continue

  summary_pre="${SUMMARY_DIR}/${TS_KEY}_crypto_${interval}_precheck.json"
  summary_verify="${SUMMARY_DIR}/${TS_KEY}_crypto_${interval}_verify.json"
  summary_post="${SUMMARY_DIR}/${TS_KEY}_crypto_${interval}_postcheck.json"

  if [[ "${CRYPTO_PREFLIGHT_CHECK}" == "1" ]]; then
    CURRENT_STAGE="crypto_precheck_${interval}"
    run_cmd \
      "crypto_precheck_${interval}" \
      python scripts/check_crypto_status.py \
      --config "$CONFIG_PATH" \
      --interval "$interval" \
      --universe "$CRYPTO_UNIVERSE" \
      --summary-json "$summary_pre" \
      --max-missing-ratio 1.0 \
      --max-zero-byte "$CRYPTO_MAX_ZERO_BYTE" \
      --stale-multiple "$CRYPTO_STALE_MULTIPLE" || true
  fi

  if [[ "${CRYPTO_UPDATE}" == "1" ]]; then
    CURRENT_STAGE="crypto_update_${interval}"
    if run_cmd "crypto_update_${interval}" tradinglab-data --config "$CONFIG_PATH" crypto update --interval "$interval" --universe "$CRYPTO_UNIVERSE"; then
      :
    else
      rc=$?
      mark_fail "crypto_update_failed rc=${rc} interval=${interval} universe=${CRYPTO_UNIVERSE}"
      exit "$rc"
    fi
  fi

  VERIFY_CRYPTO_ARGS=(
    "python" "scripts/check_crypto_status.py"
    "--config" "$CONFIG_PATH"
    "--interval" "${interval}"
    "--universe" "${CRYPTO_UNIVERSE}"
    "--summary-json" "${summary_verify}"
    "--max-missing-ratio" "${CRYPTO_MAX_MISSING_RATIO}"
    "--max-zero-byte" "${CRYPTO_MAX_ZERO_BYTE}"
    "--stale-multiple" "${CRYPTO_STALE_MULTIPLE}"
  )
  if [[ "${CRYPTO_REPAIR}" == "1" ]]; then
    VERIFY_CRYPTO_ARGS+=("--repair")
  fi

  CURRENT_STAGE="crypto_verify_fix_${interval}"
  run_cmd "crypto_verify_fix_${interval}" "${VERIFY_CRYPTO_ARGS[@]}" || true

  CURRENT_STAGE="crypto_postcheck_${interval}"
  if run_cmd \
    "crypto_postcheck_${interval}" \
    python scripts/check_crypto_status.py \
    --config "$CONFIG_PATH" \
    --interval "$interval" \
    --universe "$CRYPTO_UNIVERSE" \
    --summary-json "$summary_post" \
    --fail-on-issues \
    --max-missing-ratio "$CRYPTO_MAX_MISSING_RATIO" \
    --max-zero-byte "$CRYPTO_MAX_ZERO_BYTE" \
    --stale-multiple "$CRYPTO_STALE_MULTIPLE"; then
    :
  else
    rc=$?
    mark_fail "$(printf 'crypto_postcheck_failed rc=%s interval=%s universe=%s\nprecheck=%s\nverify=%s\npostcheck=%s' "$rc" "$interval" "$CRYPTO_UNIVERSE" "$summary_pre" "$summary_verify" "$summary_post")"
    exit "$rc"
  fi
done

rm -f "$RUNNING_FILE" "$FAIL_FILE"
{
  echo "ok"
  echo "timestamp=$TS_KEY"
  echo "log=$LOG_PATH"
  echo "summary_dir=$SUMMARY_DIR"
} > "$OK_FILE"
FINALIZED=1
log INFO "Crypto update/verify finished ok"
