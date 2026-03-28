from __future__ import annotations

import argparse
from pathlib import Path

from .config import Config, default_config_path, ticker_overrides_path, universe_dir_path
from .schema import render_schema_json, render_schema_markdown
from .store_report import generate_parquet_store_report
from .universe_build import build_universe
from .workflows import monitor_extended_hours_from_config, update_from_config


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tradinglab-data", description="Standalone data maintenance package for TradingLab parquet/universe artifacts.")
    ap.add_argument("--config", default=str(default_config_path()), help="YAML config path")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_update = sub.add_parser("update", help="Update daily parquet and extended-hours intraday parquet")
    p_update.add_argument("--symbols", nargs="*", default=None, help="Optional symbol subset")

    p_monitor = sub.add_parser("monitor-extended-hours", help="Refresh extended-hours intraday parquet and reports")
    p_monitor.add_argument("--symbols", nargs="*", default=None, help="Optional symbol subset")
    p_monitor.add_argument("--top-n", type=int, default=25)
    p_monitor.add_argument("--session", default="all", choices=["all", "pre", "post", "regular", "closed"])

    p_build = sub.add_parser("build-universe", help="Build a merged universe CSV from index sources/overrides")
    p_build.add_argument("--indices", nargs="+", required=True, help="Indices to include, e.g. sp500 djia dax mdax atx")
    p_build.add_argument("--out", required=True, help="Output CSV path")
    p_build.add_argument("--overrides-dir", default="")
    p_build.add_argument("--inactive-too", action="store_true", help="Keep inactive rows")

    p_schema = sub.add_parser("schema", help="Print parquet schema specification")
    p_schema.add_argument("--format", default="markdown", choices=["markdown", "json"])
    p_schema.add_argument("--out", default="", help="Optional output path")

    p_report = sub.add_parser("report-parquet-store", help="Audit the parquet store and write an integrity report")
    p_report.add_argument("--out-dir", default="", help="Optional report output directory")
    p_report.add_argument("--format", default="both", choices=["both", "json", "markdown"])

    args = ap.parse_args(argv)

    if args.cmd == "schema":
        text = render_schema_markdown() if args.format == "markdown" else render_schema_json()
        if args.out:
            Path(args.out).write_text(text, encoding="utf-8")
        else:
            print(text)
        return 0

    cfg = Config.load(args.config)

    if args.cmd == "build-universe":
        build_universe(
            indices=args.indices,
            out_path=args.out,
            active_only=not args.inactive_too,
            overrides_dir=args.overrides_dir or str(universe_dir_path(cfg)),
            ticker_overrides_path=ticker_overrides_path(cfg),
        )
        return 0

    if args.cmd == "update":
        update_from_config(cfg, symbols_override=args.symbols)
        return 0
    if args.cmd == "monitor-extended-hours":
        monitor_extended_hours_from_config(cfg, symbols_override=args.symbols, top_n=int(args.top_n), session_filter=str(args.session))
        return 0
    if args.cmd == "report-parquet-store":
        write_json = args.format in {"both", "json"}
        write_markdown = args.format in {"both", "markdown"}
        report = generate_parquet_store_report(
            cfg,
            out_dir=args.out_dir or None,
            write_json=write_json,
            write_markdown=write_markdown,
        )
        print(f"[PARQUET_STORE_REPORT] json={report['json_path']} markdown={report['markdown_path']}")
        return 0
    raise SystemExit(f"Unknown command: {args.cmd}")
