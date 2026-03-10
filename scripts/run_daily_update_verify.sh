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

VENV_PATH="$(resolve_venv_path || true)"
CONFIG_PATH="${TLD_CONFIG_PATH:-configs/config.yaml}"
VERIFY_INTRADAY="${TLD_VERIFY_INTRADAY:-1}"
INTRADAY_INTERVALS="${TLD_INTRADAY_INTERVALS:-5m,1m}"
MIN_PARQUET_FILES="${TLD_MIN_PARQUET_FILES:-400}"
MAX_ZERO_BYTE="${TLD_MAX_ZERO_BYTE:-0}"
MAX_MISSING_RATIO="${TLD_MAX_MISSING_RATIO:-0.20}"
MAX_DROP_RATIO="${TLD_MAX_DROP_RATIO:-0.10}"
SAMPLE_READ_FILES="${TLD_SAMPLE_READ_FILES:-30}"
VERIFY_YF="${TLD_VERIFY_YF:-1}"
FAIL_SEVERITY="${TLD_FAIL_SEVERITY:-critical}"
UNKNOWN_LARGE_GAPS_CRITICAL="${TLD_UNKNOWN_LARGE_GAPS_CRITICAL:-0}"
ETF_LARGE_GAP_TOLERANCE="${TLD_ETF_LARGE_GAP_TOLERANCE:-2}"
ETF_MAX_LARGE_GAPS_PER_YEAR="${TLD_ETF_MAX_LARGE_GAPS_PER_YEAR:-3.0}"
INTRADAY_CLEAN_CACHE="${TLD_INTRADAY_CLEAN_CACHE:-1}"
INTRADAY_LARGE_GAPS_CRITICAL="${TLD_INTRADAY_LARGE_GAPS_CRITICAL:-0}"
PROVIDER_BASELINE="${TLD_PROVIDER_BASELINE:-mixed}"
VERBOSE="${TL_VERBOSE:-1}"
DATE_KEY="$(date +%F)"
TS_KEY="$(date +%Y%m%dT%H%M%S)"
CURRENT_STAGE="startup"
FINALIZED=0

log() { echo "[$(date '+%F %T')] [$1] ${*:2}"; }

mark_fail() {
  local message="$1"
  rm -f "$RUNNING_FILE"
  printf '%s\n' "$message" > "$FAIL_FILE"
  FINALIZED=1
}

on_exit() {
  local rc="$?"
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

cfg_get() {
  "$VENV_PATH/bin/python" - "$CONFIG_PATH" "$1" "$2" "$3" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, str((Path(sys.argv[1]).resolve().parent.parent / 'src')))
from tradinglab_data.config import Config  # noqa: E402
cfg = Config.load(sys.argv[1])
print(cfg.get(sys.argv[2], sys.argv[3], default=sys.argv[4]))
PY
}

cfg_path() {
  "$VENV_PATH/bin/python" - "$CONFIG_PATH" "$1" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, str((Path(sys.argv[1]).resolve().parent.parent / 'src')))
from tradinglab_data.config import Config, intraday_root_path, parquet_root_path, registry_root_path, runs_root_path, universe_dir_path  # noqa: E402
cfg = Config.load(sys.argv[1])
name = sys.argv[2]
mapping = {
  'parquet_root': parquet_root_path,
  'universe_dir': universe_dir_path,
  'intraday_root': intraday_root_path,
  'runs_root': runs_root_path,
  'registry_root': registry_root_path,
}
print(mapping[name](cfg))
PY
}

PARQUET_ROOT="${TLD_PARQUET_ROOT:-$(cfg_path parquet_root)}"
UNIVERSE_DIR="${TLD_UNIVERSE_DIR:-$(cfg_path universe_dir)}"
INTRADAY_ROOT="${TLD_INTRADAY_ROOT:-$(cfg_path intraday_root)}"
RUNS_ROOT="${TLD_RUNS_ROOT:-$(cfg_path runs_root)}"
REGISTRY_ROOT="${TLD_REGISTRY_ROOT:-$(cfg_path registry_root)}"
LOG_DIR="${TLD_LOG_DIR:-${REGISTRY_ROOT}/logs}"
GATE_DIR="${TLD_GATE_DIR:-${REGISTRY_ROOT}/update_gate}"
SUMMARY_DIR="${GATE_DIR}/summaries"
LOG_PATH="${LOG_DIR}/update_verify_${TS_KEY}.log"
OK_FILE="${GATE_DIR}/${DATE_KEY}.ok"
FAIL_FILE="${GATE_DIR}/${DATE_KEY}.fail"
RUNNING_FILE="${GATE_DIR}/${DATE_KEY}.running"
SUMMARY_PATH="${SUMMARY_DIR}/${TS_KEY}.json"

mkdir -p "$LOG_DIR" "$GATE_DIR" "$SUMMARY_DIR"
exec >> "$LOG_PATH" 2>&1

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

CURRENT_STAGE="update"
if run_cmd "update" tradinglab-data --config "$CONFIG_PATH" update; then
  :
else
  rc=$?
  mark_fail "update_failed rc=$rc"
  exit "$rc"
fi

VERIFY_ARGS=(
  "scripts/check_parquet_status.py"
  "--config" "$CONFIG_PATH"
  "--root" "${PARQUET_ROOT}"
  "--issues-only"
  "--universe-dir" "${UNIVERSE_DIR}"
  "--summary-json" "${SUMMARY_PATH}"
  "--fail-on-issues"
  "--fail-severity" "${FAIL_SEVERITY}"
  "--etf-large-gap-tolerance" "${ETF_LARGE_GAP_TOLERANCE}"
  "--etf-max-large-gaps-per-year" "${ETF_MAX_LARGE_GAPS_PER_YEAR}"
  "--gate-min-files" "${MIN_PARQUET_FILES}"
  "--gate-max-zero-byte" "${MAX_ZERO_BYTE}"
  "--gate-max-missing-ratio" "${MAX_MISSING_RATIO}"
  "--gate-sample-read-files" "${SAMPLE_READ_FILES}"
  "--gate-max-drop-ratio" "${MAX_DROP_RATIO}"
  "--provider-baseline" "${PROVIDER_BASELINE}"
)
if [[ "${UNKNOWN_LARGE_GAPS_CRITICAL}" == "1" ]]; then
  VERIFY_ARGS+=("--large-gaps-critical")
else
  VERIFY_ARGS+=("--no-large-gaps-critical")
fi
if [[ "${VERIFY_YF}" == "1" ]]; then
  VERIFY_ARGS+=("--verify-yf")
fi

CURRENT_STAGE="verify_daily"
if run_cmd "verify_daily" "${VERIFY_ARGS[@]}"; then
  :
else
  rc=$?
  mark_fail "$(printf 'verify_failed rc=%s\nsummary=%s' "$rc" "$SUMMARY_PATH")"
  exit "$rc"
fi

if [[ "${VERIFY_INTRADAY}" == "1" ]]; then
  IFS=',' read -r -a _TLD_INTERVALS <<< "$INTRADAY_INTERVALS"
  for interval in "${_TLD_INTERVALS[@]}"; do
    interval="$(echo "$interval" | xargs)"
    [[ -z "$interval" ]] && continue
    root_i="${INTRADAY_ROOT}/${interval}"
    [[ ! -d "$root_i" ]] && continue
    count_i="$(find "$root_i" -maxdepth 1 -name '*.parquet' | wc -l | tr -d ' ')"
    [[ "$count_i" == "0" ]] && continue

    summary_i="${SUMMARY_DIR}/${TS_KEY}_intraday_${interval}.json"
    VERIFY_INTRADAY_ARGS=(
      "scripts/check_parquet_status.py"
      "--config" "$CONFIG_PATH"
      "--root" "${root_i}"
      "--parquet-kind" "intraday"
      "--issues-only"
      "--universe-dir" "${UNIVERSE_DIR}"
      "--summary-json" "${summary_i}"
      "--fail-on-issues"
      "--fail-severity" "${FAIL_SEVERITY}"
      "--etf-large-gap-tolerance" "${ETF_LARGE_GAP_TOLERANCE}"
      "--etf-max-large-gaps-per-year" "${ETF_MAX_LARGE_GAPS_PER_YEAR}"
      "--provider-baseline" "${PROVIDER_BASELINE}"
    )
    if [[ "${interval}" == "1m" ]]; then
      VERIFY_INTRADAY_ARGS+=(
        "--gate-min-files" "0"
        "--gate-max-zero-byte" "${MAX_ZERO_BYTE}"
        "--gate-max-missing-ratio" "1.0"
        "--gate-max-drop-ratio" "1.0"
        "--gate-sample-read-files" "${SAMPLE_READ_FILES}"
      )
    else
      VERIFY_INTRADAY_ARGS+=(
        "--gate-min-files" "${MIN_PARQUET_FILES}"
        "--gate-max-zero-byte" "${MAX_ZERO_BYTE}"
        "--gate-max-missing-ratio" "${MAX_MISSING_RATIO}"
        "--gate-max-drop-ratio" "${MAX_DROP_RATIO}"
        "--gate-sample-read-files" "${SAMPLE_READ_FILES}"
      )
    fi
    if [[ "${INTRADAY_CLEAN_CACHE}" == "1" ]]; then
      VERIFY_INTRADAY_ARGS+=("--clean-intraday-cache")
    fi
    if [[ "${VERIFY_YF}" == "1" ]]; then
      VERIFY_INTRADAY_ARGS+=("--verify-yf")
    fi
    if [[ "${UNKNOWN_LARGE_GAPS_CRITICAL}" == "1" ]]; then
      VERIFY_INTRADAY_ARGS+=("--large-gaps-critical")
    else
      VERIFY_INTRADAY_ARGS+=("--no-large-gaps-critical")
    fi
    if [[ "${INTRADAY_LARGE_GAPS_CRITICAL}" == "1" ]]; then
      VERIFY_INTRADAY_ARGS+=("--intraday-large-gaps-critical")
    else
      VERIFY_INTRADAY_ARGS+=("--no-intraday-large-gaps-critical")
    fi

    CURRENT_STAGE="verify_intraday_${interval}"
    if run_cmd "verify_intraday_${interval}" "${VERIFY_INTRADAY_ARGS[@]}"; then
      :
    else
      rc=$?
      mark_fail "$(printf 'verify_intraday_failed rc=%s interval=%s\nsummary=%s' "$rc" "$interval" "$summary_i")"
      exit "$rc"
    fi
  done
fi

rm -f "$RUNNING_FILE" "$FAIL_FILE"
{
  echo "ok"
  echo "timestamp=$TS_KEY"
  echo "summary=$SUMMARY_PATH"
  echo "log=$LOG_PATH"
} > "$OK_FILE"
FINALIZED=1
log INFO "Nightly update/verify finished ok"
