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

CONFIG_PATH="$(resolve_config_path)"
VERIFY_INTRADAY="${TLD_VERIFY_INTRADAY:-1}"
INTRADAY_INTERVALS="${TLD_INTRADAY_INTERVALS:-5m,1m}"
MIN_PARQUET_FILES="${TLD_MIN_PARQUET_FILES:-400}"
MAX_ZERO_BYTE="${TLD_MAX_ZERO_BYTE:-0}"
MAX_MISSING_RATIO="${TLD_MAX_MISSING_RATIO:-0.20}"
MAX_DROP_RATIO="${TLD_MAX_DROP_RATIO:-0.10}"
SAMPLE_READ_FILES="${TLD_SAMPLE_READ_FILES:-30}"
VERIFY_YF="${TLD_VERIFY_YF:-1}"
VERIFY_CRYPTO="${TLD_VERIFY_CRYPTO:-1}"
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
FAIL_SEVERITY="${TLD_FAIL_SEVERITY:-critical}"
UNKNOWN_LARGE_GAPS_CRITICAL="${TLD_UNKNOWN_LARGE_GAPS_CRITICAL:-0}"
ETF_LARGE_GAP_TOLERANCE="${TLD_ETF_LARGE_GAP_TOLERANCE:-2}"
ETF_MAX_LARGE_GAPS_PER_YEAR="${TLD_ETF_MAX_LARGE_GAPS_PER_YEAR:-3.0}"
INTRADAY_CLEAN_CACHE="${TLD_INTRADAY_CLEAN_CACHE:-1}"
INTRADAY_LARGE_GAPS_CRITICAL="${TLD_INTRADAY_LARGE_GAPS_CRITICAL:-0}"
PROVIDER_BASELINE="${TLD_PROVIDER_BASELINE:-mixed}"
INTRADAY_1M_GATE_UNIVERSE="${TLD_1M_GATE_UNIVERSE:-sp500}"
INTRADAY_1M_STRICT_BLOCKING="${TLD_1M_STRICT_BLOCKING:-0}"
INTRADAY_1M_QUARANTINE_DAYS="${TLD_1M_QUARANTINE_DAYS:-1}"
INTRADAY_1M_QUARANTINE_FAIL_STREAK="${TLD_1M_QUARANTINE_FAIL_STREAK:-2}"
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

resolve_1m_universe_csv() {
  local raw="${INTRADAY_1M_GATE_UNIVERSE:-}"
  if [[ -z "$raw" ]]; then
    return 1
  fi
  if [[ -f "$raw" ]]; then
    printf '%s\n' "$raw"
    return 0
  fi
  if [[ -f "${UNIVERSE_DIR}/${raw}.csv" ]]; then
    printf '%s\n' "${UNIVERSE_DIR}/${raw}.csv"
    return 0
  fi
  return 1
}

build_1m_filtered_universe() {
  local base_csv="$1"
  local out_csv="$2"
  "$VENV_PATH/bin/python" - "$base_csv" "$INTRADAY_1M_QUARANTINE_PATH" "$out_csv" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import polars as pl

base_csv = Path(sys.argv[1])
quarantine_path = Path(sys.argv[2])
out_csv = Path(sys.argv[3])

df = pl.read_csv(base_csv)
if "symbol" not in df.columns:
    raise SystemExit(f"missing symbol column in {base_csv}")

now = datetime.now(timezone.utc)
quarantine = {}
if quarantine_path.exists():
    try:
        payload = json.loads(quarantine_path.read_text(encoding="utf-8"))
        quarantine = payload.get("symbols", {}) if isinstance(payload, dict) else {}
    except Exception:
        quarantine = {}

blocked: set[str] = set()
for symbol, item in quarantine.items():
    if not isinstance(item, dict):
        continue
    until = str(item.get("quarantine_until", "")).strip()
    if not until:
        continue
    try:
        dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
    except Exception:
        continue
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt > now:
        blocked.add(str(symbol).strip().upper())

filtered = df.filter(~pl.col("symbol").cast(pl.String).str.to_uppercase().is_in(list(blocked)))
out_csv.parent.mkdir(parents=True, exist_ok=True)
filtered.write_csv(out_csv)
print(f"[1m-gate] base={df.height} filtered={filtered.height} quarantined={len(blocked)} out={out_csv}")
PY
}

update_1m_quarantine_from_summary() {
  local summary_json="$1"
  "$VENV_PATH/bin/python" - "$summary_json" "$INTRADAY_1M_QUARANTINE_PATH" "$INTRADAY_1M_QUARANTINE_DAYS" "$INTRADAY_1M_QUARANTINE_FAIL_STREAK" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

summary_path = Path(sys.argv[1])
quarantine_path = Path(sys.argv[2])
quarantine_days = max(1, int(sys.argv[3]))
fail_streak_threshold = max(1, int(sys.argv[4]))

if not summary_path.exists():
    print(f"[1m-gate] summary missing: {summary_path}")
    raise SystemExit(0)

summary = json.loads(summary_path.read_text(encoding="utf-8"))
critical = [str(s).strip().upper() for s in summary.get("issue_symbols_critical", []) if str(s).strip()]
all_issues = [str(s).strip().upper() for s in summary.get("issue_symbols", []) if str(s).strip()]
failing = set(critical)
now = datetime.now(timezone.utc)

payload = {"updated_at": now.isoformat(timespec="seconds"), "symbols": {}}
if quarantine_path.exists():
    try:
        current = json.loads(quarantine_path.read_text(encoding="utf-8"))
        if isinstance(current, dict) and isinstance(current.get("symbols"), dict):
            payload = current
    except Exception:
        pass

symbols = payload.setdefault("symbols", {})

for sym, item in list(symbols.items()):
    if not isinstance(item, dict):
        symbols.pop(sym, None)
        continue
    until = str(item.get("quarantine_until", "")).strip()
    if not until:
        continue
    try:
        until_dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
        if until_dt.tzinfo is None:
            until_dt = until_dt.replace(tzinfo=timezone.utc)
        if until_dt <= now and sym not in failing:
            symbols.pop(sym, None)
    except Exception:
        if sym not in failing:
            symbols.pop(sym, None)

for sym in failing:
    row = symbols.get(sym) if isinstance(symbols.get(sym), dict) else {}
    fail_streak = int(row.get("fail_streak", 0)) + 1
    row["fail_streak"] = fail_streak
    row["last_failed_at"] = now.isoformat(timespec="seconds")
    if fail_streak >= fail_streak_threshold:
        row["quarantine_until"] = (now + timedelta(days=quarantine_days)).isoformat(timespec="seconds")
    symbols[sym] = row

for sym, item in list(symbols.items()):
    if sym in failing:
        continue
    if not isinstance(item, dict):
        symbols.pop(sym, None)
        continue
    if not str(item.get("quarantine_until", "")).strip():
        item["fail_streak"] = 0

payload["updated_at"] = now.isoformat(timespec="seconds")
quarantine_path.parent.mkdir(parents=True, exist_ok=True)
quarantine_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
active = sum(1 for v in symbols.values() if isinstance(v, dict) and str(v.get("quarantine_until", "")).strip())
print(f"[1m-gate] failures={len(failing)} quarantined_active={active} file={quarantine_path}")
PY
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
from tradinglab_data.config import Config, crypto_root_path, intraday_root_path, parquet_root_path, registry_root_path, runs_root_path, universe_dir_path  # noqa: E402
cfg = Config.load(sys.argv[1])
name = sys.argv[2]
mapping = {
  'parquet_root': parquet_root_path,
  'universe_dir': universe_dir_path,
  'intraday_root': intraday_root_path,
  'crypto_root': crypto_root_path,
  'runs_root': runs_root_path,
  'registry_root': registry_root_path,
}
print(mapping[name](cfg))
PY
}

PARQUET_ROOT="${TLD_PARQUET_ROOT:-$(cfg_path parquet_root)}"
UNIVERSE_DIR="${TLD_UNIVERSE_DIR:-$(cfg_path universe_dir)}"
INTRADAY_ROOT="${TLD_INTRADAY_ROOT:-$(cfg_path intraday_root)}"
CRYPTO_ROOT="${TLD_CRYPTO_ROOT:-$(cfg_path crypto_root)}"
RUNS_ROOT="${TLD_RUNS_ROOT:-$(cfg_path runs_root)}"
REGISTRY_ROOT="${TLD_REGISTRY_ROOT:-$(cfg_path registry_root)}"
LOG_DIR="${TLD_LOG_DIR:-${REGISTRY_ROOT}/logs}"
GATE_DIR="${TLD_GATE_DIR:-${REGISTRY_ROOT}/update_gate}"
LOCK_DIR="${REGISTRY_ROOT}/locks"
LOCK_PATH="${LOCK_DIR}/daily_update_verify.lockdir"
SUMMARY_DIR="${GATE_DIR}/summaries"
LOG_PATH="${LOG_DIR}/update_verify_${TS_KEY}.log"
OK_FILE="${GATE_DIR}/${DATE_KEY}.ok"
FAIL_FILE="${GATE_DIR}/${DATE_KEY}.fail"
RUNNING_FILE="${GATE_DIR}/${DATE_KEY}.running"
SUMMARY_PATH="${SUMMARY_DIR}/${TS_KEY}.json"
INTRADAY_1M_QUARANTINE_PATH="${TLD_1M_QUARANTINE_PATH:-${GATE_DIR}/intraday_1m_quarantine.json}"

mkdir -p "$LOG_DIR" "$GATE_DIR" "$SUMMARY_DIR" "$LOCK_DIR"
exec >> "$LOG_PATH" 2>&1

if ! mkdir "$LOCK_PATH" 2>/dev/null; then
  log ERROR "another update/verify run already holds lock: $LOCK_PATH"
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
    intraday_universe_csv=""
    if [[ "${interval}" == "1m" ]]; then
      if one_m_base_csv="$(resolve_1m_universe_csv)"; then
        one_m_filtered_csv="${SUMMARY_DIR}/${TS_KEY}_intraday_1m_gate_universe.csv"
        build_1m_filtered_universe "$one_m_base_csv" "$one_m_filtered_csv"
        intraday_universe_csv="$one_m_filtered_csv"
      else
        log WARN "1m gate universe '${INTRADAY_1M_GATE_UNIVERSE}' not found; using default active universe CSV."
      fi
    fi
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
    if [[ -n "${intraday_universe_csv}" ]]; then
      VERIFY_INTRADAY_ARGS+=("--universe" "${intraday_universe_csv}")
    fi
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
      if [[ "${interval}" == "1m" && -f "${summary_i}" ]]; then
        update_1m_quarantine_from_summary "${summary_i}" || true
      fi
      :
    else
      rc=$?
      if [[ "${interval}" == "1m" && -f "${summary_i}" ]]; then
        update_1m_quarantine_from_summary "${summary_i}" || true
      fi
      if [[ "${interval}" == "1m" && "${INTRADAY_1M_STRICT_BLOCKING}" != "1" ]]; then
        log WARN "1m intraday gate failed (rc=${rc}) but strict blocking is disabled; continuing workflow."
        continue
      fi
      mark_fail "$(printf 'verify_intraday_failed rc=%s interval=%s\nsummary=%s' "$rc" "$interval" "$summary_i")"
      exit "$rc"
    fi
  done
fi

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

if [[ "${CRYPTO_UPDATE}" == "1" ]]; then
  IFS=',' read -r -a _TLD_CRYPTO_UPDATE_INTERVALS <<< "$CRYPTO_INTERVALS"
  for interval in "${_TLD_CRYPTO_UPDATE_INTERVALS[@]}"; do
    interval="$(echo "$interval" | xargs)"
    [[ -z "$interval" ]] && continue

    CURRENT_STAGE="crypto_update_${interval}"
    if run_cmd "crypto_update_${interval}" tradinglab-data --config "$CONFIG_PATH" crypto update --interval "$interval" --universe "$CRYPTO_UNIVERSE"; then
      :
    else
      rc=$?
      mark_fail "crypto_update_failed rc=${rc} interval=${interval} universe=${CRYPTO_UNIVERSE}"
      exit "$rc"
    fi
  done
fi

if [[ "${VERIFY_CRYPTO}" == "1" ]] && [[ -d "${CRYPTO_ROOT}" ]]; then
  IFS=',' read -r -a _TLD_CRYPTO_INTERVALS <<< "$CRYPTO_INTERVALS"
  for interval in "${_TLD_CRYPTO_INTERVALS[@]}"; do
    interval="$(echo "$interval" | xargs)"
    [[ -z "$interval" ]] && continue

    summary_crypto="${SUMMARY_DIR}/${TS_KEY}_crypto_${interval}.json"
    VERIFY_CRYPTO_ARGS=(
      "scripts/check_crypto_status.py"
      "--config" "$CONFIG_PATH"
      "--interval" "${interval}"
      "--universe" "${CRYPTO_UNIVERSE}"
      "--summary-json" "${summary_crypto}"
      "--fail-on-issues"
      "--max-missing-ratio" "${CRYPTO_MAX_MISSING_RATIO}"
      "--max-zero-byte" "${CRYPTO_MAX_ZERO_BYTE}"
      "--stale-multiple" "${CRYPTO_STALE_MULTIPLE}"
    )
    if [[ "${CRYPTO_REPAIR}" == "1" ]]; then
      VERIFY_CRYPTO_ARGS+=("--repair")
    fi

    CURRENT_STAGE="verify_crypto_${interval}"
    if run_cmd "verify_crypto_${interval}" "${VERIFY_CRYPTO_ARGS[@]}"; then
      :
    else
      rc=$?
      mark_fail "$(printf 'verify_crypto_failed rc=%s interval=%s universe=%s\nsummary=%s' "$rc" "$interval" "$CRYPTO_UNIVERSE" "$summary_crypto")"
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
