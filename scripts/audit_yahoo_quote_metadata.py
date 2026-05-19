#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tradinglab_data.config import Config, default_config_path, universe_dir_path
from tradinglab_data.yahoo_quote_audit import (
    DEFAULT_ETF_MASTER_PATH_NAME,
    YAHOO_FETCH_MODES,
    audit_rows_to_csv,
    audit_rows_to_json,
    audit_rows_to_markdown,
    audit_universe_file,
    make_browser_snapshot_fetcher,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit local ETF metadata against Yahoo Finance quote-page exchange and currency fields."
    )
    parser.add_argument("--config", default=str(default_config_path()), help="Path to configuration YAML")
    parser.add_argument(
        "--path",
        type=Path,
        default=None,
        help="ETF master CSV path (defaults to paths.universe_dir/etf_all.csv)",
    )
    parser.add_argument("--symbols", nargs="*", default=None, help="Optional exact symbols to audit")
    parser.add_argument("--timeout", type=float, default=20.0, help="Per-symbol Yahoo HTTP timeout in seconds")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Optional pause between symbol requests")
    parser.add_argument("--fetcher", choices=YAHOO_FETCH_MODES, default="browser")
    parser.add_argument("--format", choices=("markdown", "json", "csv"), default="markdown")
    parser.add_argument("--out", default="", help="Optional output file path")
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Exit non-zero when any row is not a clean match",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    cfg = Config.load(args.config)
    target = args.path or (universe_dir_path(cfg) / DEFAULT_ETF_MASTER_PATH_NAME)
    custom_fetcher = None
    custom_fetcher_close = None
    if args.fetcher == "browser":
        custom_fetcher, custom_fetcher_close = make_browser_snapshot_fetcher(timeout=float(args.timeout))
    try:
        rows = audit_universe_file(
            target,
            symbols=[str(symbol).strip().upper() for symbol in (args.symbols or []) if str(symbol).strip()],
            timeout=float(args.timeout),
            sleep_seconds=float(args.sleep_seconds),
            fetcher=custom_fetcher,
        )
    finally:
        if custom_fetcher_close is not None:
            custom_fetcher_close()

    if args.format == "json":
        rendered = audit_rows_to_json(rows)
    elif args.format == "csv":
        rendered = audit_rows_to_csv(rows)
    else:
        rendered = audit_rows_to_markdown(rows)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")

    if bool(args.fail_on_mismatch) and any(row.status != "match" for row in rows):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
