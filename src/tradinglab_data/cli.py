from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

from .config import Config, default_config_path, ticker_overrides_path, universe_dir_path
from .consistency_report import (
    generate_universe_consistency_report,
    render_universe_consistency_json,
    render_universe_consistency_markdown,
)
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
from .schema import render_schema_json, render_schema_markdown
from .store_report import generate_parquet_store_report
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
            result = intraday_sync_from_config(cfg, universe=universe, symbols_override=symbols, full_window=True)
            print(
                f"[INTRADAY_SYNC_BACKFILL] interval={result['interval']} "
                f"live_files_written={result['live']['files_written']} "
                f"research_files_written={result['research']['files_written']} "
                f"symbols={len(result['symbols'])} fetched_symbols={result['fetched_symbols']} "
                f"live_root={result['live']['root']} research_root={result['research']['root']} "
                f"universe={result['universe']}"
            )
            return 0
        if args.intraday_sync_cmd == "update":
            result = intraday_sync_from_config(cfg, universe=universe, symbols_override=symbols, full_window=False)
            print(
                f"[INTRADAY_SYNC_UPDATE] interval={result['interval']} "
                f"live_files_written={result['live']['files_written']} "
                f"research_files_written={result['research']['files_written']} "
                f"symbols={len(result['symbols'])} fetched_symbols={result['fetched_symbols']} "
                f"live_root={result['live']['root']} research_root={result['research']['root']} "
                f"universe={result['universe']}"
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
