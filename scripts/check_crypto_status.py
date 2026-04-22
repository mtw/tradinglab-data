#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parent.parent / "src")))

from tradinglab_data.config import Config, default_config_path
from tradinglab_data.crypto.verify import CryptoVerifyConfig, run_crypto_verify_checks


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify crypto parquet coverage and optionally repair dirty symbols.")
    ap.add_argument("--config", default=str(default_config_path()), help="YAML config path")
    ap.add_argument("--exchange", default="", help="Optional crypto exchange override")
    ap.add_argument("--interval", required=True, choices=["1d", "1h", "15m"])
    ap.add_argument("--universe", default="", help="Crypto universe to verify")
    ap.add_argument("--summary-json", default="", help="Optional summary JSON output path")
    ap.add_argument("--repair", action="store_true", help="Attempt single-symbol repair for dirty symbols")
    ap.add_argument("--fail-on-issues", action="store_true", help="Exit non-zero when issues remain")
    ap.add_argument("--max-missing-ratio", type=float, default=0.0)
    ap.add_argument("--max-zero-byte", type=int, default=0)
    ap.add_argument("--stale-multiple", type=int, default=2)
    args = ap.parse_args()

    cfg = Config.load(args.config)
    result = run_crypto_verify_checks(
        cfg,
        CryptoVerifyConfig(
            interval=str(args.interval),
            universe=str(args.universe or "").strip() or None,
            exchange=str(args.exchange or "").strip() or None,
            max_missing_ratio=float(args.max_missing_ratio),
            max_zero_byte=int(args.max_zero_byte),
            stale_multiple=int(args.stale_multiple),
            repair=bool(args.repair),
        ),
    )
    if str(args.summary_json).strip():
        out_path = Path(str(args.summary_json))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(
        f"[CRYPTO_VERIFY] ok={result['ok']} interval={result['interval']} "
        f"expected={result['expected_symbols']} present={result['files_present']} "
        f"dirty={len(result['dirty_symbols'])} repaired={len(result['repaired_symbols'])}"
    )
    if args.fail_on_issues and not result["ok"]:
        raise SystemExit("\n".join(result["errors"] or ["crypto verification failed"]))


if __name__ == "__main__":
    main()
