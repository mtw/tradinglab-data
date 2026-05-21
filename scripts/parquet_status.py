#!/usr/bin/env python
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tradinglab_data.config import (  # noqa: E402
    Config,
    default_config_path,
    intraday_live_root_path,
    intraday_research_root_path,
    parquet_root_path,
    ticker_overrides_path,
    universe_dir_path,
)
from tradinglab_data.consistency_report import generate_universe_consistency_report  # noqa: E402
from tradinglab_data.intraday_live import (  # noqa: E402
    inspect_intraday_live_store,
    validate_intraday_live_store,
)
from tradinglab_data.intraday_research import (  # noqa: E402
    inspect_intraday_research_store,
    validate_intraday_research_store,
)
from tradinglab_data.universe import load_universe_frame  # noqa: E402


def _load_check_module():
    path = ROOT / "scripts" / "check_parquet_status.py"
    module_name = "scripts_check_parquet_status_runtime"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_universe_symbols(cfg: Config, universe_name: str) -> list[str]:
    path = universe_dir_path(cfg) / f"{universe_name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Universe shard not found: {path}")
    try:
        overrides_path: str | Path | None = ticker_overrides_path(cfg)
    except ValueError:
        overrides_path = None
    frame = load_universe_frame(path, ticker_overrides_path=overrides_path)
    symbols = [str(symbol).strip().upper() for symbol in frame.get_column("symbol").to_list() if str(symbol).strip()]
    if not symbols:
        raise ValueError(f"Universe {universe_name} resolved no symbols.")
    return symbols


def _status_issue_count(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return int(frame.filter(pl.col("status") != "ok").height)


def _status_missing_count(frame: pl.DataFrame) -> int:
    if frame.is_empty() or "exists" not in frame.columns:
        return 0
    return int(frame.filter(pl.col("exists") == False).height)  # noqa: E712


def _run_check(check_mod, argv: list[str]) -> int:
    try:
        return int(check_mod.main(argv))
    except SystemExit as exc:
        code = exc.code
        if isinstance(code, int):
            return code
        return 1


def _run_daily(cfg: Config, *, check_mod, instrument_type: str, summary_path: Path | None) -> int:
    print("== Daily parquet consistency ==")
    check_args = [
        "--config",
        str(cfg.source_path or default_config_path()),
        "--root",
        str(parquet_root_path(cfg)),
        "--parquet-kind",
        "daily",
        "--with-meta",
        "--issues-only",
        "--fail-on-issues",
        "--fail-severity",
        "critical",
    ]
    check_exit = _run_check(check_mod, check_args)

    consistency = generate_universe_consistency_report(
        cfg,
        dataset="daily",
        instrument_type=(instrument_type or None),
    )
    missing_count = _status_missing_count(consistency)
    issue_count = _status_issue_count(consistency)
    ok = (check_exit == 0) and missing_count == 0 and issue_count == 0

    print("\nDaily completeness")
    print(f"  symbols_checked: {consistency.height}")
    print(f"  missing_files: {missing_count}")
    print(f"  issue_rows: {issue_count}")
    print(f"  ok: {ok}")

    if summary_path is not None:
        payload = {
            "mode": "daily",
            "root": str(parquet_root_path(cfg)),
            "instrument_type": instrument_type,
            "check_exit": check_exit,
            "symbols_checked": int(consistency.height),
            "missing_files": missing_count,
            "issue_rows": issue_count,
            "ok": ok,
        }
        summary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote summary: {summary_path}")

    return 0 if ok else 2


def _run_intraday(
    cfg: Config,
    *,
    universe_name: str,
    interval: str,
    summary_path: Path | None,
) -> int:
    symbols = _load_universe_symbols(cfg, universe_name)
    research_root = intraday_research_root_path(cfg)
    live_root = intraday_live_root_path(cfg)

    print(f"== Intraday parquet consistency ({universe_name}, {interval}) ==")

    research_validate = validate_intraday_research_store(
        symbols,
        research_root=research_root,
        interval=interval,
        universe_name=universe_name,
    )
    live_validate = validate_intraday_live_store(
        symbols,
        live_root=live_root,
        interval=interval,
        universe_name=universe_name,
    )
    research_inspect = inspect_intraday_research_store(symbols, research_root=research_root, interval=interval)
    live_inspect = inspect_intraday_live_store(symbols, live_root=live_root, interval=interval)

    research_dirty = len(research_validate["dirty_files"])
    live_dirty = len(live_validate["dirty_files"])
    research_missing = sum(1 for item in research_inspect if not bool(item.get("exists")))
    live_missing = sum(1 for item in live_inspect if not bool(item.get("exists")))
    research_invalid = sum(1 for item in research_inspect if bool(item.get("exists")) and not bool(item.get("valid")))
    live_invalid = sum(1 for item in live_inspect if bool(item.get("exists")) and not bool(item.get("valid")))
    ok = bool(research_validate["ok"]) and bool(live_validate["ok"])

    if research_dirty:
        print("\nResearch dirty files")
        for path in research_validate["dirty_files"]:
            print(f"  {path}")
    if live_dirty:
        print("\nLive dirty files")
        for path in live_validate["dirty_files"]:
            print(f"  {path}")

    print("\nIntraday completeness")
    print(f"  symbols_checked: {len(symbols)}")
    print(f"  research_missing_files: {research_missing}")
    print(f"  research_invalid_files: {research_invalid}")
    print(f"  research_dirty_files: {research_dirty}")
    print(f"  live_missing_files: {live_missing}")
    print(f"  live_invalid_files: {live_invalid}")
    print(f"  live_dirty_files: {live_dirty}")
    print(f"  ok: {ok}")

    if summary_path is not None:
        payload = {
            "mode": "intraday",
            "universe": universe_name,
            "interval": interval,
            "symbols_checked": len(symbols),
            "research_root": str(research_root / interval),
            "live_root": str(live_root / interval),
            "research_missing_files": research_missing,
            "research_invalid_files": research_invalid,
            "research_dirty_files": research_validate["dirty_files"],
            "live_missing_files": live_missing,
            "live_invalid_files": live_invalid,
            "live_dirty_files": live_validate["dirty_files"],
            "ok": ok,
        }
        summary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote summary: {summary_path}")

    return 0 if ok else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Simple operator wrapper for daily or intraday parquet consistency and completeness checks."
    )
    parser.add_argument("--config", default=str(default_config_path()), help="YAML config path")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--daily", action="store_true", help="Check daily OHLC parquet consistency and completeness.")
    mode.add_argument("--intraday", action="store_true", help="Check intraday research/live parquet consistency and completeness.")
    parser.add_argument("--universe", default="intraday_live_core", help="Intraday universe shard name (default: intraday_live_core).")
    parser.add_argument("--interval", default="5m", help="Intraday interval directory (default: 5m).")
    parser.add_argument("--instrument-type", default="", help="Optional daily universe filter, e.g. stock or etf.")
    parser.add_argument("--summary-json", default="", help="Optional output path for a compact JSON summary.")
    args = parser.parse_args(argv)

    cfg = Config.load(args.config)
    summary_path = Path(args.summary_json) if str(args.summary_json).strip() else None
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)

    check_mod = _load_check_module()
    if bool(args.daily):
        return _run_daily(
            cfg,
            check_mod=check_mod,
            instrument_type=str(args.instrument_type).strip().lower(),
            summary_path=summary_path,
        )
    return _run_intraday(
        cfg,
        universe_name=str(args.universe).strip(),
        interval=str(args.interval).strip(),
        summary_path=summary_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
