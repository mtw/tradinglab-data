from __future__ import annotations

import argparse
import json
import random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yfinance as yf

from tradinglab_data._intraday_fetch import MAX_PERIOD_BY_INTERVAL, period_for_interval
from tradinglab_data._yf_utils import classify_yf_download_issue, run_yf_download, share_class_fallback, yf_date_window
from tradinglab_data.config import Config, ticker_overrides_path, universe_csv_path, universe_dir_path
from tradinglab_data.universe import load_universe


@dataclass(frozen=True)
class ProbeResult:
    symbol: str
    interval: str
    ok: bool
    rows: int
    status: str
    issue: str
    used_fallback_symbol: str


def _load_symbols(cfg: Config, indices: list[str] | None) -> list[str]:
    if not indices:
        return load_universe(
            universe_csv_path(cfg),
            universe_dir=universe_dir_path(cfg),
            ticker_overrides_path=ticker_overrides_path(cfg),
        )

    root = universe_dir_path(cfg)
    symbols: list[str] = []
    seen: set[str] = set()
    for index_name in indices:
        shard = root / f"{index_name}.csv"
        shard_symbols = load_universe(
            shard,
            universe_dir=None,
            ticker_overrides_path=ticker_overrides_path(cfg),
        )
        for symbol in shard_symbols:
            if symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)
    return symbols


def _sample_symbols(symbols: list[str], sample_size: int, seed: int | None) -> list[str]:
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    if len(symbols) <= sample_size:
        return list(symbols)
    rng = random.Random(seed) if seed is not None else random.Random()
    return sorted(rng.sample(symbols, sample_size))


def _download_params(interval: str, lookback_days: int, *, prepost: bool) -> dict[str, Any]:
    if interval in MAX_PERIOD_BY_INTERVAL:
        return {
            "period": period_for_interval(interval, MAX_PERIOD_BY_INTERVAL, purpose="access check"),
            "interval": interval,
            "prepost": prepost,
        }
    start_s, end_s = yf_date_window(lookback_days)
    return {
        "start": start_s,
        "end": end_s,
        "interval": interval,
    }


def probe_symbol_interval(symbol: str, interval: str, lookback_days: int, *, prepost: bool) -> ProbeResult:
    params = _download_params(interval, lookback_days, prepost=prepost)
    try:
        df_pd, output, exc = run_yf_download(
            yf.download,
            symbol,
            auto_adjust=False,
            progress=False,
            group_by="column",
            threads=False,
            **params,
        )
    except Exception as exc:
        return ProbeResult(
            symbol=symbol,
            interval=interval,
            ok=False,
            rows=0,
            status="other_error",
            issue=str(exc),
            used_fallback_symbol="",
        )
    issue = classify_yf_download_issue(f"{output}\n{exc!s}" if exc is not None else output)
    if exc is not None and issue is None:
        return ProbeResult(
            symbol=symbol,
            interval=interval,
            ok=False,
            rows=0,
            status="other_error",
            issue=str(exc),
            used_fallback_symbol="",
        )
    used_fallback = ""
    if issue is not None:
        return ProbeResult(
            symbol=symbol,
            interval=interval,
            ok=False,
            rows=0,
            status="connectivity_error",
            issue=issue,
            used_fallback_symbol=used_fallback,
        )

    rows = len(df_pd) if df_pd is not None else 0
    if rows > 0:
        return ProbeResult(
            symbol=symbol,
            interval=interval,
            ok=True,
            rows=rows,
            status="ok",
            issue="",
            used_fallback_symbol=used_fallback,
        )

    fallback = share_class_fallback(symbol)
    if fallback and fallback != symbol:
        try:
            df_pd_alt, output_alt, exc_alt = run_yf_download(
                yf.download,
                fallback,
                auto_adjust=False,
                progress=False,
                group_by="column",
                threads=False,
                **params,
            )
        except Exception as exc:
            return ProbeResult(
                symbol=symbol,
                interval=interval,
                ok=False,
                rows=0,
                status="other_error",
                issue=str(exc),
                used_fallback_symbol=fallback,
            )
        issue_alt = classify_yf_download_issue(f"{output_alt}\n{exc_alt!s}" if exc_alt is not None else output_alt)
        if exc_alt is not None and issue_alt is None:
            return ProbeResult(
                symbol=symbol,
                interval=interval,
                ok=False,
                rows=0,
                status="other_error",
                issue=str(exc_alt),
                used_fallback_symbol=fallback,
            )
        if issue_alt is not None:
            return ProbeResult(
                symbol=symbol,
                interval=interval,
                ok=False,
                rows=0,
                status="connectivity_error",
                issue=issue_alt,
                used_fallback_symbol=fallback,
            )
        rows_alt = len(df_pd_alt) if df_pd_alt is not None else 0
        if rows_alt > 0:
            return ProbeResult(
                symbol=symbol,
                interval=interval,
                ok=True,
                rows=rows_alt,
                status="ok",
                issue="",
                used_fallback_symbol=fallback,
            )
        used_fallback = fallback

    return ProbeResult(
        symbol=symbol,
        interval=interval,
        ok=False,
        rows=0,
        status="empty",
        issue="",
        used_fallback_symbol=used_fallback,
    )


def _summarize(results: list[ProbeResult]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for result in results:
        bucket = summary.setdefault(
            result.interval,
            {"ok": 0, "empty": 0, "connectivity_error": 0, "other_error": 0},
        )
        bucket[result.status] = bucket.get(result.status, 0) + 1
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe Yahoo Finance accessibility across a random sample of configured universe symbols."
    )
    parser.add_argument("--config", default="configs/config.yaml", help="Path to tradinglab-data YAML config.")
    parser.add_argument(
        "--indices",
        default="",
        help="Comma-separated universe shard names to sample from, e.g. sp500,djia. Default: full configured universe.",
    )
    parser.add_argument("--sample-size", type=int, default=15, help="Number of symbols to probe. Default: 15.")
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible sampling. Default: unset, so each run samples different symbols.",
    )
    parser.add_argument(
        "--intervals",
        default="1d,5m,1m",
        help="Comma-separated Yahoo intervals to probe. Default: 1d,5m,1m.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Lookback window for non-intraday intervals. Default: 30.",
    )
    parser.add_argument(
        "--no-prepost",
        action="store_true",
        help="Disable pre/post market data for intraday probes.",
    )
    parser.add_argument("--json-out", default="", help="Optional JSON output path.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    cfg = Config.load(args.config)
    indices = [value.strip() for value in str(args.indices).split(",") if value.strip()]
    intervals = [value.strip() for value in str(args.intervals).split(",") if value.strip()]
    symbols = _load_symbols(cfg, indices)
    if not symbols:
        print(
            f"[ERROR] no symbols loaded from {'full universe' if not indices else ','.join(indices)}; "
            "check the configured universe inputs",
        )
        return 2
    sample = _sample_symbols(symbols, args.sample_size, args.seed)
    if not sample:
        print("[ERROR] empty Yahoo probe sample; check --sample-size and universe inputs")
        return 2

    print(
        f"[INFO] probing Yahoo access for {len(sample)} symbols across {len(intervals)} intervals "
        f"from {'full universe' if not indices else ','.join(indices)}"
    )

    results: list[ProbeResult] = []
    for symbol in sample:
        for interval in intervals:
            result = probe_symbol_interval(
                symbol,
                interval,
                args.lookback_days,
                prepost=not args.no_prepost,
            )
            results.append(result)
            fallback_note = f" fallback={result.used_fallback_symbol}" if result.used_fallback_symbol else ""
            issue_note = f" issue={result.issue}" if result.issue else ""
            print(
                f"[{result.status.upper()}] symbol={result.symbol} interval={result.interval} rows={result.rows}"
                f"{fallback_note}{issue_note}"
            )

    summary = _summarize(results)
    print("\nSummary:")
    for interval in intervals:
        bucket = summary.get(interval, {})
        print(
            f"- {interval}: ok={bucket.get('ok', 0)} "
            f"empty={bucket.get('empty', 0)} "
            f"connectivity_error={bucket.get('connectivity_error', 0)} "
            f"other_error={bucket.get('other_error', 0)}"
        )

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(
                {
                    "config": str(cfg.source_path or args.config),
                    "sample_size": len(sample),
                    "seed": args.seed,
                    "intervals": intervals,
                    "indices": indices,
                    "results": [asdict(result) for result in results],
                    "summary": summary,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    total = len(results)
    connectivity_errors = sum(1 for result in results if result.status == "connectivity_error")
    if total > 0 and connectivity_errors == total:
        return 2
    if connectivity_errors > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
