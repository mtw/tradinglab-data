from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

from .config import (
    Config,
    default_config_path,
    exchange_defaults_path,
    fx_daily_root_path,
    symbol_master_path,
    symbol_overrides_path,
    ticker_overrides_path,
    universe_csv_path,
    universe_dir_path,
)
from .consistency_report import (
    generate_universe_consistency_report,
    render_universe_consistency_json,
    render_universe_consistency_markdown,
)
from .contracts import IntradayDualSyncResult
from .crypto.workflows import (
    crypto_backfill_from_config,
    crypto_diff_universe_from_config,
    crypto_inspect_from_config,
    crypto_list_symbols_from_config,
    crypto_prune_from_config,
    crypto_refresh_universe_from_config,
    crypto_show_universe_from_config,
    crypto_validate_from_config,
)
from .fx import available_fx_pairs, load_fx_pair, sync_fx_pair_yahoo, validate_fx_pair
from .schema import render_schema_json, render_schema_markdown
from .store_report import generate_parquet_store_report
from .symbol_master import (
    build_symbol_master_frame,
    inspect_symbol_master_frame,
    load_symbol_master_frame,
    load_symbol_overrides,
    require_exchange_defaults_frame,
    validate_symbol_master,
    write_symbol_master_frame,
)
from .universe import load_universe_frame
from .universe_build import build_universe
from .workflows import (
    backfill_extended_hours_from_config,
    intraday_live_inspect_from_config,
    intraday_live_update_from_config,
    intraday_live_validate_from_config,
    intraday_research_inspect_from_config,
    intraday_research_update_from_config,
    intraday_research_validate_from_config,
    intraday_sync_from_config,
    monitor_extended_hours_from_config,
    update_from_config,
)


def _display_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _render_symbol_master_markdown(frame, *, exchange: str | None = None, fx_pair: str | None = None, issues: str | None = None) -> str:
    scope = []
    if exchange:
        scope.append(f"exchange={exchange.upper()}")
    if fx_pair:
        scope.append(f"fx_pair={fx_pair.upper()}")
    if issues:
        scope.append(f"issues={issues}")
    header = ", ".join(scope) or "all"
    lines = [
        "# Symbol Master Inspection",
        "",
        f"- scope: `{header}`",
        f"- rows: `{frame.height}`",
        "",
        "| Symbol | Exchange | Country | Asset Currency | FX Pair | Metadata Source | Metadata Quality |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in frame.iter_rows(named=True):
        lines.append(
            "| {symbol} | {exchange} | {country} | {asset_currency} | {fx_pair_to_base} | {metadata_source} | {metadata_quality} |".format(
                **{key: _display_value(value) for key, value in row.items()}
            )
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tradinglab-data", description="Standalone data maintenance package for parquet and universe artifacts.")
    ap.add_argument("--config", default=str(default_config_path()), help="YAML config path")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_update = sub.add_parser("update", help="Update daily parquet and extended-hours intraday parquet")
    p_update.add_argument("--symbols", nargs="*", default=None, help="Optional symbol subset")

    p_monitor = sub.add_parser("monitor-extended-hours", help="Refresh extended-hours intraday parquet and reports")
    p_monitor.add_argument("--symbols", nargs="*", default=None, help="Optional symbol subset")
    p_monitor.add_argument("--top-n", type=int, default=25)
    p_monitor.add_argument("--session", default="all", choices=["all", "pre", "post", "regular", "closed"])

    p_backfill_intraday = sub.add_parser("backfill-extended-hours", help="Backfill extended-hours intraday parquet for one interval using the provider's full allowed window")
    p_backfill_intraday.add_argument("--interval", required=True, choices=["5m", "1m"])
    p_backfill_intraday.add_argument("--symbols", nargs="*", default=None, help="Optional symbol subset")

    p_intraday = sub.add_parser("intraday", help="General intraday research-store workflows")
    intraday_sub = p_intraday.add_subparsers(dest="intraday_cmd", required=True)

    p_intraday_backfill = intraday_sub.add_parser("backfill", help="Backfill the intraday research store using the provider's full allowed window")
    p_intraday_backfill.add_argument("--universe", default="")
    p_intraday_backfill.add_argument("--symbols", nargs="*", default=None)

    p_intraday_update = intraday_sub.add_parser("update", help="Incrementally refresh the intraday research store")
    p_intraday_update.add_argument("--universe", default="")
    p_intraday_update.add_argument("--symbols", nargs="*", default=None)

    p_intraday_validate = intraday_sub.add_parser("validate", help="Validate the local intraday research parquet files")
    p_intraday_validate.add_argument("--universe", default="")
    p_intraday_validate.add_argument("--symbols", nargs="*", default=None)

    p_intraday_inspect = intraday_sub.add_parser("inspect", help="Inspect local intraday research parquet coverage")
    p_intraday_inspect.add_argument("--universe", default="")
    p_intraday_inspect.add_argument("--symbols", nargs="*", default=None)

    p_intraday_live = sub.add_parser("intraday-live", help="Session-aware live intraday store workflows")
    intraday_live_sub = p_intraday_live.add_subparsers(dest="intraday_live_cmd", required=True)

    p_intraday_live_backfill = intraday_live_sub.add_parser("backfill", help="Backfill the live intraday store using the provider's full allowed window")
    p_intraday_live_backfill.add_argument("--universe", default="")
    p_intraday_live_backfill.add_argument("--symbols", nargs="*", default=None)

    p_intraday_live_update = intraday_live_sub.add_parser("update", help="Incrementally refresh the live intraday store")
    p_intraday_live_update.add_argument("--universe", default="")
    p_intraday_live_update.add_argument("--symbols", nargs="*", default=None)

    p_intraday_live_validate = intraday_live_sub.add_parser("validate", help="Validate the local live intraday parquet files")
    p_intraday_live_validate.add_argument("--universe", default="")
    p_intraday_live_validate.add_argument("--symbols", nargs="*", default=None)

    p_intraday_live_inspect = intraday_live_sub.add_parser("inspect", help="Inspect local live intraday parquet coverage")
    p_intraday_live_inspect.add_argument("--universe", default="")
    p_intraday_live_inspect.add_argument("--symbols", nargs="*", default=None)

    p_intraday_sync = sub.add_parser("intraday-sync", help="Fetch once and write both the session-aware live store and regular-session research store")
    intraday_sync_sub = p_intraday_sync.add_subparsers(dest="intraday_sync_cmd", required=True)

    p_intraday_sync_backfill = intraday_sync_sub.add_parser("backfill", help="Backfill live and research stores using the provider's full allowed window")
    p_intraday_sync_backfill.add_argument("--universe", default="")
    p_intraday_sync_backfill.add_argument("--symbols", nargs="*", default=None)

    p_intraday_sync_update = intraday_sync_sub.add_parser("update", help="Incrementally refresh live and research stores from one shared fetch")
    p_intraday_sync_update.add_argument("--universe", default="")
    p_intraday_sync_update.add_argument("--symbols", nargs="*", default=None)

    p_build = sub.add_parser("build-universe", help="Build a merged universe CSV from index sources/overrides")
    p_build.add_argument("--indices", nargs="+", required=True, help="Indices to include, e.g. sp500 djia dax mdax atx")
    p_build.add_argument("--out", required=True, help="Output CSV path")
    p_build.add_argument("--overrides-dir", default="")
    p_build.add_argument("--inactive-too", action="store_true", help="Keep inactive rows")

    p_schema = sub.add_parser("schema", help="Print parquet schema specification")
    p_schema.add_argument("--format", default="markdown", choices=["markdown", "json"])
    p_schema.add_argument("--out", default="", help="Optional output path")

    p_build_symbol_master = sub.add_parser("build-symbol-master", help="Build the authoritative symbol master CSV")
    p_build_symbol_master.add_argument("--base-currency", default="EUR")
    p_build_symbol_master.add_argument("--universe-csv", default="")
    p_build_symbol_master.add_argument("--exchange-defaults", default="")
    p_build_symbol_master.add_argument("--symbol-overrides", default="")
    p_build_symbol_master.add_argument("--output", default="")
    p_build_symbol_master.add_argument("--strict", dest="strict", action="store_true")
    p_build_symbol_master.add_argument("--no-strict", dest="strict", action="store_false")
    p_build_symbol_master.set_defaults(strict=True)

    p_validate_symbol_master = sub.add_parser("validate-symbol-master", help="Validate a symbol master CSV")
    p_validate_symbol_master.add_argument("--path", default="")
    p_validate_symbol_master.add_argument("--strict", action="store_true")

    p_inspect_symbol_master = sub.add_parser("inspect-symbol-master", help="Inspect symbol master rows for review")
    p_inspect_symbol_master.add_argument("--path", default="")
    p_inspect_symbol_master.add_argument("--exchange", default="")
    p_inspect_symbol_master.add_argument("--fx-pair", default="")
    p_inspect_symbol_master.add_argument("--issues", default="")
    p_inspect_symbol_master.add_argument("--symbols", nargs="*", default=None)
    p_inspect_symbol_master.add_argument("--limit", type=int, default=0)
    p_inspect_symbol_master.add_argument("--format", default="markdown", choices=["markdown", "json", "csv"])
    p_inspect_symbol_master.add_argument("--out", default="")

    p_fx_backfill = sub.add_parser("fx-backfill", help="Fetch and write daily FX parquet files")
    p_fx_backfill.add_argument("--pairs", nargs="+", required=True)
    p_fx_backfill.add_argument("--start", default="")
    p_fx_backfill.add_argument("--end", default="")
    p_fx_backfill.add_argument("--provider", default="yahoo")
    p_fx_backfill.add_argument("--allow-inverse", dest="allow_inverse", action="store_true")
    p_fx_backfill.add_argument("--no-allow-inverse", dest="allow_inverse", action="store_false")
    p_fx_backfill.set_defaults(allow_inverse=True)

    p_fx_update = sub.add_parser("fx-update", help="Refresh daily FX parquet files")
    p_fx_update.add_argument("--pairs", nargs="*", default=None)
    p_fx_update.add_argument("--provider", default="yahoo")
    p_fx_update.add_argument("--allow-inverse", dest="allow_inverse", action="store_true")
    p_fx_update.add_argument("--no-allow-inverse", dest="allow_inverse", action="store_false")
    p_fx_update.set_defaults(allow_inverse=True)

    p_fx_validate = sub.add_parser("fx-validate", help="Validate local daily FX parquet files")
    p_fx_validate.add_argument("--pairs", nargs="*", default=None)

    p_fx_inspect = sub.add_parser("fx-inspect", help="Inspect local daily FX parquet coverage")
    p_fx_inspect.add_argument("--pairs", nargs="*", default=None)
    p_fx_inspect.add_argument("--tail", type=int, default=5)

    p_report = sub.add_parser("report-parquet-store", help="Audit the parquet store and write an integrity report")
    p_report.add_argument("--out-dir", default="", help="Optional report output directory")
    p_report.add_argument("--format", default="both", choices=["both", "json", "markdown"])

    p_consistency = sub.add_parser("report-universe-consistency", help="Report symbol-level parquet coverage and health for a universe slice")
    p_consistency.add_argument("--dataset", required=True, choices=["daily", "intraday", "crypto"])
    p_consistency.add_argument("--interval", default="", help="Required for intraday and crypto reports")
    p_consistency.add_argument("--universe", default="", help="Crypto universe name")
    p_consistency.add_argument("--exchange", default="", help="Crypto exchange override")
    p_consistency.add_argument("--instrument-type", default="", help="Optional daily/intraday filter, e.g. stock or etf")
    p_consistency.add_argument("--symbols", nargs="*", default=None)
    p_consistency.add_argument("--format", default="markdown", choices=["markdown", "json", "csv"])
    p_consistency.add_argument("--out", default="", help="Optional output path")

    p_crypto = sub.add_parser("crypto", help="Crypto universe and parquet workflows")
    crypto_sub = p_crypto.add_subparsers(dest="crypto_cmd", required=True)

    p_crypto_list = crypto_sub.add_parser("list-symbols", help="List available crypto symbols from the configured provider")
    p_crypto_list.add_argument("--exchange", default="")

    p_crypto_backfill = crypto_sub.add_parser("backfill", help="Backfill crypto OHLCV parquet files")
    p_crypto_backfill.add_argument("--exchange", default="")
    p_crypto_backfill.add_argument("--interval", required=True)
    p_crypto_backfill.add_argument("--universe", default="")
    p_crypto_backfill.add_argument("--symbols", nargs="*", default=None)

    p_crypto_update = crypto_sub.add_parser("update", help="Incrementally refresh crypto OHLCV parquet files")
    p_crypto_update.add_argument("--exchange", default="")
    p_crypto_update.add_argument("--interval", required=True)
    p_crypto_update.add_argument("--universe", default="")
    p_crypto_update.add_argument("--symbols", nargs="*", default=None)

    p_crypto_validate = crypto_sub.add_parser("validate", help="Validate crypto OHLCV parquet files already written locally")
    p_crypto_validate.add_argument("--exchange", default="")
    p_crypto_validate.add_argument("--interval", required=True)
    p_crypto_validate.add_argument("--universe", default="")
    p_crypto_validate.add_argument("--symbols", nargs="*", default=None)

    p_crypto_refresh = crypto_sub.add_parser("refresh-universe", help="Refresh a dynamic crypto universe from metadata")
    p_crypto_refresh.add_argument("--exchange", default="")
    p_crypto_refresh.add_argument("--provider", default="coingecko")
    p_crypto_refresh.add_argument("--universe", default="crypto_high_liquidity")
    p_crypto_refresh.add_argument("--limit", type=int, default=0)

    p_crypto_show = crypto_sub.add_parser("show-universe", help="Print symbols in a crypto universe")
    p_crypto_show.add_argument("--exchange", default="")
    p_crypto_show.add_argument("--universe", default="")
    p_crypto_show.add_argument("--symbols", nargs="*", default=None)

    p_crypto_diff = crypto_sub.add_parser("diff-universe", help="Diff two crypto universes")
    p_crypto_diff.add_argument("--exchange", default="")
    p_crypto_diff.add_argument("--left-universe", required=True)
    p_crypto_diff.add_argument("--right-universe", required=True)

    p_crypto_inspect = crypto_sub.add_parser("inspect", help="Inspect local crypto parquet coverage for a universe")
    p_crypto_inspect.add_argument("--exchange", default="")
    p_crypto_inspect.add_argument("--interval", required=True)
    p_crypto_inspect.add_argument("--universe", default="")
    p_crypto_inspect.add_argument("--symbols", nargs="*", default=None)

    p_crypto_prune = crypto_sub.add_parser("prune", help="Prune local crypto parquet files not present in a universe")
    p_crypto_prune.add_argument("--exchange", default="")
    p_crypto_prune.add_argument("--interval", required=True)
    p_crypto_prune.add_argument("--universe", default="")
    p_crypto_prune.add_argument("--symbols", nargs="*", default=None)
    p_crypto_prune.add_argument("--apply", action="store_true")

    args = ap.parse_args(argv)

    if args.cmd == "schema":
        text = render_schema_markdown() if args.format == "markdown" else render_schema_json()
        if args.out:
            Path(args.out).write_text(text, encoding="utf-8")
        else:
            print(text)
        return 0

    cfg = Config.load(args.config)

    if args.cmd == "build-symbol-master":
        strict = bool(args.strict)
        universe_csv = Path(args.universe_csv) if args.universe_csv else universe_csv_path(cfg)
        defaults_path = Path(args.exchange_defaults) if args.exchange_defaults else exchange_defaults_path(cfg)
        overrides_path = Path(args.symbol_overrides) if args.symbol_overrides else symbol_overrides_path(cfg)
        output_path = Path(args.output) if args.output else symbol_master_path(cfg)
        universe_frame = load_universe_frame(
            universe_csv,
            universe_dir=universe_dir_path(cfg),
            ticker_overrides_path=ticker_overrides_path(cfg),
        )
        defaults = require_exchange_defaults_frame(defaults_path, strict=strict)
        overrides = load_symbol_overrides(overrides_path)
        built = build_symbol_master_frame(
            universe_frame,
            exchange_defaults=defaults,
            symbol_overrides=overrides,
            base_currency=str(args.base_currency),
            strict=strict,
        )
        write_symbol_master_frame(built, output_path)
        missing_required = len(validate_symbol_master(output_path, strict=False))
        print(
            f"[BUILD_SYMBOL_MASTER] symbols={built.height} base_currency={str(args.base_currency).upper()} "
            f"missing_required={missing_required} output={output_path} strict={strict}"
        )
        return 0

    if args.cmd == "validate-symbol-master":
        target_path = Path(args.path) if args.path else symbol_master_path(cfg)
        errors = validate_symbol_master(target_path, strict=bool(args.strict))
        if errors:
            raise SystemExit("\n".join(errors))
        frame = load_symbol_master_frame(target_path, strict=False)
        print(f"[VALIDATE_SYMBOL_MASTER] rows={frame.height} path={target_path}")
        return 0

    if args.cmd == "inspect-symbol-master":
        target_path = Path(args.path) if args.path else symbol_master_path(cfg)
        frame = inspect_symbol_master_frame(
            target_path,
            exchange=str(args.exchange or "").strip() or None,
            fx_pair=str(args.fx_pair or "").strip() or None,
            issues=str(args.issues or "").strip() or None,
            symbols=getattr(args, "symbols", None),
        ).sort(["exchange", "symbol"])
        if int(args.limit) > 0:
            frame = frame.head(int(args.limit))
        if args.format == "markdown":
            text = _render_symbol_master_markdown(
                frame,
                exchange=str(args.exchange or "").strip() or None,
                fx_pair=str(args.fx_pair or "").strip() or None,
                issues=str(args.issues or "").strip() or None,
            )
            if args.out:
                Path(args.out).write_text(text, encoding="utf-8")
            else:
                print(text)
            return 0
        if args.format == "json":
            text = frame.write_json()
            if args.out:
                Path(args.out).write_text(text, encoding="utf-8")
            else:
                print(text)
            return 0
        if args.out:
            frame.write_csv(args.out)
        else:
            print(frame.write_csv())
        return 0

    if args.cmd == "fx-backfill":
        root = fx_daily_root_path(cfg)
        for pair in args.pairs:
            fx_result = sync_fx_pair_yahoo(
                str(pair),
                root,
                start=str(args.start or "").strip() or None,
                end=str(args.end or "").strip() or None,
                provider=str(args.provider),
                allow_inverse=bool(args.allow_inverse),
            )
            print(
                f"[FX_SYNC] pair={fx_result['pair']} rows_written={fx_result['rows_written']} "
                f"path={fx_result['path']} source_symbol={fx_result['source_symbol']} used_inverse={fx_result['used_inverse']}"
            )
        return 0

    if args.cmd == "fx-update":
        root = fx_daily_root_path(cfg)
        pairs = list(args.pairs or [])
        if not pairs:
            symbol_master = load_symbol_master_frame(symbol_master_path(cfg), strict=True)
            pairs = sorted(
                {
                    str(value)
                    for value in symbol_master.get_column("fx_pair_to_base").to_list()
                    if str(value) and str(value)[:3] != str(value)[3:]
                }
            )
        for pair in pairs:
            fx_result = sync_fx_pair_yahoo(str(pair), root, provider=str(args.provider), allow_inverse=bool(args.allow_inverse))
            print(
                f"[FX_UPDATE] pair={fx_result['pair']} rows_written={fx_result['rows_written']} "
                f"path={fx_result['path']} source_symbol={fx_result['source_symbol']} used_inverse={fx_result['used_inverse']}"
            )
        return 0

    if args.cmd == "fx-validate":
        root = fx_daily_root_path(cfg)
        pairs = list(args.pairs or []) or available_fx_pairs(root)
        failures: list[str] = []
        for pair in pairs:
            errors = validate_fx_pair(root, str(pair))
            if errors:
                failures.extend(f"{pair}: {error}" for error in errors)
            else:
                print(f"[FX_VALIDATE] pair={str(pair).upper()} ok=1")
        if failures:
            raise SystemExit("\n".join(failures))
        return 0

    if args.cmd == "fx-inspect":
        root = fx_daily_root_path(cfg)
        pairs = list(args.pairs or []) or available_fx_pairs(root)
        for pair in pairs:
            frame = load_fx_pair(root, str(pair), strict=False)
            tail = frame.tail(int(args.tail))
            latest_close = tail.get_column("close").to_list()[-1] if not tail.is_empty() else None
            start = frame.get_column("date").min() if not frame.is_empty() else None
            end = frame.get_column("date").max() if not frame.is_empty() else None
            print(
                f"{str(pair).upper()} rows={frame.height} start={_display_value(start)} "
                f"end={_display_value(end)} latest_close={_display_value(latest_close)}"
            )
        return 0

    if args.cmd == "intraday":
        universe = str(getattr(args, "universe", "") or "").strip() or None
        symbols = getattr(args, "symbols", None)
        if args.intraday_cmd == "backfill":
            result = intraday_research_update_from_config(cfg, universe=universe, symbols_override=symbols, full_window=True)
            print(
                f"[INTRADAY_BACKFILL] interval={result['interval']} files_written={result['files_written']} "
                f"symbols={len(result['symbols'])} root={result['root']} universe={result['universe']}"
            )
            return 0
        if args.intraday_cmd == "update":
            result = intraday_research_update_from_config(cfg, universe=universe, symbols_override=symbols, full_window=False)
            print(
                f"[INTRADAY_UPDATE] interval={result['interval']} files_written={result['files_written']} "
                f"symbols={len(result['symbols'])} root={result['root']} universe={result['universe']}"
            )
            return 0
        if args.intraday_cmd == "validate":
            result = intraday_research_validate_from_config(cfg, universe=universe, symbols_override=symbols)
            if not result["ok"]:
                raise SystemExit("\n".join(result["errors"] or ["intraday research validation failed"]))
            print(
                f"[INTRADAY_VALIDATE] interval={result['interval']} files_checked={result['files_checked']} "
                f"root={result['root']} universe={result['universe']}"
            )
            return 0
        if args.intraday_cmd == "inspect":
            for item in intraday_research_inspect_from_config(cfg, universe=universe, symbols_override=symbols):
                print(
                    f"{item['symbol']} exists={item['exists']} rows={item['rows']} valid={item['valid']} "
                    f"start={item['start'] or '-'} end={item['end'] or '-'} path={item['path']}"
                )
            return 0

    if args.cmd == "intraday-live":
        universe = str(getattr(args, "universe", "") or "").strip() or None
        symbols = getattr(args, "symbols", None)
        if args.intraday_live_cmd == "backfill":
            result = intraday_live_update_from_config(cfg, universe=universe, symbols_override=symbols, full_window=True)
            print(
                f"[INTRADAY_LIVE_BACKFILL] interval={result['interval']} files_written={result['files_written']} "
                f"symbols={len(result['symbols'])} root={result['root']} universe={result['universe']}"
            )
            return 0
        if args.intraday_live_cmd == "update":
            result = intraday_live_update_from_config(cfg, universe=universe, symbols_override=symbols, full_window=False)
            print(
                f"[INTRADAY_LIVE_UPDATE] interval={result['interval']} files_written={result['files_written']} "
                f"symbols={len(result['symbols'])} root={result['root']} universe={result['universe']}"
            )
            return 0
        if args.intraday_live_cmd == "validate":
            result = intraday_live_validate_from_config(cfg, universe=universe, symbols_override=symbols)
            if not result["ok"]:
                raise SystemExit("\n".join(result["errors"] or ["intraday live validation failed"]))
            print(
                f"[INTRADAY_LIVE_VALIDATE] interval={result['interval']} files_checked={result['files_checked']} "
                f"root={result['root']} universe={result['universe']}"
            )
            return 0
        if args.intraday_live_cmd == "inspect":
            for item in intraday_live_inspect_from_config(cfg, universe=universe, symbols_override=symbols):
                print(
                    f"{item['symbol']} exists={item['exists']} rows={item['rows']} valid={item['valid']} "
                    f"start={item['start'] or '-'} end={item['end'] or '-'} path={item['path']}"
                )
            return 0

    if args.cmd == "intraday-sync":
        universe = str(getattr(args, "universe", "") or "").strip() or None
        symbols = getattr(args, "symbols", None)
        if args.intraday_sync_cmd == "backfill":
            sync_result: IntradayDualSyncResult = intraday_sync_from_config(
                cfg,
                universe=universe,
                symbols_override=symbols,
                full_window=True,
            )
            print(
                f"[INTRADAY_SYNC_BACKFILL] interval={sync_result['interval']} "
                f"live_files_written={sync_result['live']['files_written']} "
                f"research_files_written={sync_result['research']['files_written']} "
                f"symbols={len(sync_result['symbols'])} fetched_symbols={sync_result['fetched_symbols']} "
                f"live_root={sync_result['live']['root']} research_root={sync_result['research']['root']} "
                f"universe={sync_result['universe']}"
            )
            return 0
        if args.intraday_sync_cmd == "update":
            sync_result = cast(
                IntradayDualSyncResult,
                intraday_sync_from_config(cfg, universe=universe, symbols_override=symbols, full_window=False),
            )
            print(
                f"[INTRADAY_SYNC_UPDATE] interval={sync_result['interval']} "
                f"live_files_written={sync_result['live']['files_written']} "
                f"research_files_written={sync_result['research']['files_written']} "
                f"symbols={len(sync_result['symbols'])} fetched_symbols={sync_result['fetched_symbols']} "
                f"live_root={sync_result['live']['root']} research_root={sync_result['research']['root']} "
                f"universe={sync_result['universe']}"
            )
            return 0

    if args.cmd == "build-universe":
        build_universe(
            indices=args.indices,
            out_path=args.out,
            active_only=not args.inactive_too,
            overrides_dir=args.overrides_dir or str(universe_dir_path(cfg)),
            ticker_overrides_path=ticker_overrides_path(cfg),
        )
        return 0

    if args.cmd == "crypto":
        exchange = str(args.exchange or "").strip() or None
        universe = str(getattr(args, "universe", "") or "").strip() or None
        symbols = getattr(args, "symbols", None)
        if args.crypto_cmd == "list-symbols":
            for symbol in crypto_list_symbols_from_config(cfg, exchange=exchange):
                print(symbol)
            return 0
        if args.crypto_cmd == "backfill":
            crypto_backfill_from_config(cfg, exchange=exchange, interval=str(args.interval), universe=universe, symbols_override=symbols)
            return 0
        if args.crypto_cmd == "update":
            crypto_backfill_from_config(
                cfg,
                exchange=exchange,
                interval=str(args.interval),
                universe=universe,
                symbols_override=symbols,
                incremental=True,
            )
            return 0
        if args.crypto_cmd == "validate":
            validate_result = crypto_validate_from_config(
                cfg,
                exchange=exchange,
                interval=str(args.interval),
                universe=universe,
                symbols_override=symbols,
            )
            if not validate_result["ok"]:
                raise SystemExit("\n".join(validate_result["errors"] or ["crypto validation failed"]))
            return 0
        if args.crypto_cmd == "refresh-universe":
            refresh_result = crypto_refresh_universe_from_config(
                cfg,
                exchange=exchange,
                provider_name=str(args.provider),
                universe=str(args.universe),
                limit=int(args.limit) if int(args.limit) > 0 else None,
            )
            print(
                f"[CRYPTO_REFRESH_UNIVERSE] provider={refresh_result['provider']} "
                f"universe={refresh_result['universe']} symbols={len(refresh_result['symbols_selected'])} "
                f"registry={refresh_result['registry_path']} universe_path={refresh_result['universe_path']}"
            )
            return 0
        if args.crypto_cmd == "show-universe":
            for symbol in crypto_show_universe_from_config(cfg, exchange=exchange, universe=universe, symbols_override=symbols):
                print(symbol)
            return 0
        if args.crypto_cmd == "diff-universe":
            diff = crypto_diff_universe_from_config(
                cfg,
                exchange=exchange,
                left_universe=str(args.left_universe),
                right_universe=str(args.right_universe),
            )
            left_only = cast(list[str], diff["left_only"])
            right_only = cast(list[str], diff["right_only"])
            shared = cast(list[str], diff["shared"])
            print(f"[CRYPTO_DIFF_UNIVERSE] left={diff['left_universe']} right={diff['right_universe']}")
            print(f"left_only={','.join(left_only) or '-'}")
            print(f"right_only={','.join(right_only) or '-'}")
            print(f"shared={','.join(shared) or '-'}")
            return 0
        if args.crypto_cmd == "inspect":
            for item in crypto_inspect_from_config(
                cfg,
                exchange=exchange,
                interval=str(args.interval),
                universe=universe,
                symbols_override=symbols,
            ):
                print(
                    f"{item['symbol']} exists={item['exists']} rows={item['rows']} "
                    f"start={item['start'] or '-'} end={item['end'] or '-'} path={item['path']}"
                )
            return 0
        if args.crypto_cmd == "prune":
            pruned = crypto_prune_from_config(
                cfg,
                exchange=exchange,
                interval=str(args.interval),
                universe=universe,
                symbols_override=symbols,
                apply=bool(args.apply),
            )
            print(f"[CRYPTO_PRUNE] apply={bool(args.apply)} files={len(pruned)}")
            for path in pruned:
                print(path)
            return 0

    if args.cmd == "update":
        update_from_config(cfg, symbols_override=args.symbols)
        return 0
    if args.cmd == "monitor-extended-hours":
        monitor_extended_hours_from_config(cfg, symbols_override=args.symbols, top_n=int(args.top_n), session_filter=str(args.session))
        return 0
    if args.cmd == "backfill-extended-hours":
        result = backfill_extended_hours_from_config(cfg, interval=str(args.interval), symbols_override=args.symbols)
        print(f"[BACKFILL_EXTENDED_HOURS] interval={result['interval']} written={result['written']} symbols={result['symbols']} root={result['root']}")
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
    if args.cmd == "report-universe-consistency":
        frame = generate_universe_consistency_report(
            cfg,
            dataset=str(args.dataset),
            interval=str(args.interval or "").strip() or None,
            universe=str(args.universe or "").strip() or None,
            exchange=str(args.exchange or "").strip() or None,
            instrument_type=str(args.instrument_type or "").strip() or None,
            symbols_override=getattr(args, "symbols", None),
        )
        if args.format == "markdown":
            text = render_universe_consistency_markdown(
                frame,
                dataset=str(args.dataset),
                interval=str(args.interval or "").strip() or None,
                universe=str(args.universe or "").strip() or None,
                instrument_type=str(args.instrument_type or "").strip() or None,
            )
            if args.out:
                Path(args.out).write_text(text, encoding="utf-8")
            else:
                print(text)
            return 0
        if args.format == "json":
            text = render_universe_consistency_json(frame)
            if args.out:
                Path(args.out).write_text(text, encoding="utf-8")
            else:
                print(text)
            return 0
        if args.out:
            frame.write_csv(args.out)
        else:
            print(frame.write_csv())
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
