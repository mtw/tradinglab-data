from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from ..config import ConfigLike, crypto_root_path
from ..contracts import CryptoMetadataEntry, CryptoSyncResult, CryptoUniverseRefreshResult, CryptoValidateResult
from ..schema import CRYPTO_PARQUET_SCHEMA
from .providers.binance_ccxt import BinanceCCXTProvider
from .providers.coinbase_ccxt import CoinbaseCCXTProvider
from .providers.coingecko_provider import CoinGeckoProvider
from .providers.kraken_ccxt import KrakenCCXTProvider
from .registry import merge_dynamic_registry, resolve_crypto_universe, write_dynamic_universe
from .storage import atomic_write_parquet, crypto_parquet_path, read_crypto_parquet
from .symbols import normalize_crypto_symbol
from .validation import filter_closed_bars, merge_crypto_frames, validate_crypto_ohlcv_frame


@dataclass(frozen=True)
class _CryptoConfig:
    root: Path
    exchange: str
    market_type: str
    default_universe: str
    quote_assets: tuple[str, ...]
    max_batch_limit: int
    incremental_lookback_bars: int
    full_backfill_limit: int
    validate_continuity: bool
    universe_refresh_provider: str
    universe_refresh_limit: int
    universe_refresh_pages: int
    universe_refresh_min_market_cap: float
    universe_refresh_min_volume: float
    stablecoin_ids: tuple[str, ...]
    excluded_symbols: tuple[str, ...]
    excluded_ids: tuple[str, ...]
    exclude_wrapped_assets: bool


def _read_crypto_config(cfg: ConfigLike, exchange: str | None = None) -> _CryptoConfig:
    exchange_name = exchange or str(cfg.get("crypto", "exchange", default="binance")).strip().lower() or "binance"
    quote_assets = tuple(str(asset).strip().upper() for asset in cfg.get("crypto", "quote_assets", default=["USDT"]))
    return _CryptoConfig(
        root=crypto_root_path(cfg),
        exchange=exchange_name,
        market_type=str(cfg.get("crypto", "market_type", default="spot")).strip().lower() or "spot",
        default_universe=str(cfg.get("crypto", "default_universe", default="crypto_majors")).strip() or "crypto_majors",
        quote_assets=quote_assets or ("USDT",),
        max_batch_limit=int(cfg.get("crypto", "max_batch_limit", default=1000)),
        incremental_lookback_bars=int(cfg.get("crypto", "incremental_lookback_bars", default=500)),
        full_backfill_limit=int(cfg.get("crypto", "full_backfill_limit", default=5000)),
        validate_continuity=bool(cfg.get("crypto", "validate_continuity", default=True)),
        universe_refresh_provider=str(cfg.get("crypto", "universe_refresh_provider", default="coingecko")).strip().lower() or "coingecko",
        universe_refresh_limit=int(cfg.get("crypto", "universe_refresh_limit", default=25)),
        universe_refresh_pages=int(cfg.get("crypto", "universe_refresh_pages", default=2)),
        universe_refresh_min_market_cap=float(cfg.get("crypto", "universe_refresh_min_market_cap", default=0.0)),
        universe_refresh_min_volume=float(cfg.get("crypto", "universe_refresh_min_volume", default=0.0)),
        stablecoin_ids=tuple(str(item).strip().lower() for item in cfg.get("crypto", "stablecoin_ids", default=["tether", "usd-coin", "dai", "first-digital-usd", "true-usd", "paypal-usd", "usdd", "frax"])),
        excluded_symbols=tuple(str(item).strip().upper() for item in cfg.get("crypto", "excluded_symbols", default=[])),
        excluded_ids=tuple(str(item).strip().lower() for item in cfg.get("crypto", "excluded_ids", default=[])),
        exclude_wrapped_assets=bool(cfg.get("crypto", "exclude_wrapped_assets", default=True)),
    )


def _provider_for(crypto_cfg: _CryptoConfig) -> BinanceCCXTProvider | KrakenCCXTProvider | CoinbaseCCXTProvider:
    if crypto_cfg.exchange == "binance":
        return BinanceCCXTProvider(
            exchange_name=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            quote_assets=crypto_cfg.quote_assets,
        )
    if crypto_cfg.exchange == "kraken":
        return KrakenCCXTProvider(
            exchange_name=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            quote_assets=crypto_cfg.quote_assets,
        )
    if crypto_cfg.exchange == "coinbase":
        return CoinbaseCCXTProvider(
            exchange_name=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            quote_assets=crypto_cfg.quote_assets,
        )
    raise ValueError(f"Unsupported crypto exchange: {crypto_cfg.exchange}")


def _resolve_symbols(
    cfg: ConfigLike,
    crypto_cfg: _CryptoConfig,
    *,
    universe: str | None,
    symbols_override: list[str] | None,
) -> list[str]:
    if symbols_override:
        return [normalize_crypto_symbol(symbol) for symbol in symbols_override]
    selected_universe = universe or crypto_cfg.default_universe
    entries = resolve_crypto_universe(
        selected_universe,
        cfg=cfg,
        exchange=crypto_cfg.exchange,
        market_type=crypto_cfg.market_type,
        quote_assets=crypto_cfg.quote_assets,
    )
    return [entry["symbol_canonical"] for entry in entries]


def crypto_list_symbols_from_config(cfg: ConfigLike, *, exchange: str | None = None) -> list[str]:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    provider = _provider_for(crypto_cfg)
    return provider.list_symbols()


def crypto_backfill_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    interval: str,
    universe: str | None = None,
    symbols_override: list[str] | None = None,
    incremental: bool = False,
) -> CryptoSyncResult:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    provider = _provider_for(crypto_cfg)
    symbols = _resolve_symbols(cfg, crypto_cfg, universe=universe, symbols_override=symbols_override)
    tradable_symbols = set(provider.list_symbols())
    files_written = 0
    rows_written = 0
    unchanged_symbols: list[str] = []
    skipped_symbols: list[str] = []
    for symbol in symbols:
        out_path = crypto_parquet_path(
            crypto_cfg.root,
            exchange=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            interval=interval,
            symbol=symbol,
        )
        existing = read_crypto_parquet(out_path)
        if symbol not in tradable_symbols:
            skipped_symbols.append(symbol)
            continue
        fetch_limit = crypto_cfg.incremental_lookback_bars if incremental else crypto_cfg.full_backfill_limit
        start = None
        if incremental and existing is not None and not existing.is_empty():
            latest = existing.get_column("timestamp").max()
            latest_dt = _as_datetime(latest)
            if latest_dt is not None:
                start = latest_dt.replace(tzinfo=timezone.utc) - _interval_delta(interval)
        fetched = _fetch_symbol_history(
            provider,
            symbol,
            interval,
            start=start,
            total_limit=fetch_limit,
            batch_limit=crypto_cfg.max_batch_limit,
        )
        closed = filter_closed_bars(fetched, interval=interval)
        if incremental and existing is not None and not existing.is_empty() and closed.is_empty():
            unchanged_symbols.append(symbol)
            continue
        merged = merge_crypto_frames(existing, closed)
        if existing is not None and _frames_equal(existing, merged):
            unchanged_symbols.append(symbol)
            continue
        validate_crypto_ohlcv_frame(merged, interval=interval, require_continuity=crypto_cfg.validate_continuity)
        atomic_write_parquet(out_path, merged)
        files_written += 1
        rows_written += merged.height
    return {
        "exchange": crypto_cfg.exchange,
        "market_type": crypto_cfg.market_type,
        "interval": interval,
        "universe": universe or crypto_cfg.default_universe,
        "symbols": symbols,
        "files_written": files_written,
        "rows_written": rows_written,
        "unchanged_symbols": unchanged_symbols,
        "skipped_symbols": skipped_symbols,
        "pruned_files": [],
        "root": str(crypto_cfg.root),
    }


def crypto_validate_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    interval: str,
    universe: str | None = None,
    symbols_override: list[str] | None = None,
) -> CryptoValidateResult:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    symbols = _resolve_symbols(cfg, crypto_cfg, universe=universe, symbols_override=symbols_override)
    dirty_files: list[str] = []
    errors: list[str] = []
    for symbol in symbols:
        path = crypto_parquet_path(
            crypto_cfg.root,
            exchange=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            interval=interval,
            symbol=symbol,
        )
        if not path.exists():
            dirty_files.append(str(path))
            errors.append(f"missing_crypto_parquet:{path}")
            continue
        try:
            frame = pl.read_parquet(str(path))
            validate_crypto_ohlcv_frame(frame, interval=interval, require_continuity=crypto_cfg.validate_continuity)
        except Exception as exc:
            dirty_files.append(str(path))
            errors.append(f"invalid_crypto_parquet:{path}:{exc}")
    return {
        "ok": not errors,
        "exchange": crypto_cfg.exchange,
        "market_type": crypto_cfg.market_type,
        "interval": interval,
        "universe": universe or crypto_cfg.default_universe,
        "root": str(crypto_cfg.root),
        "files_checked": len(symbols),
        "dirty_files": dirty_files,
        "errors": errors,
    }


def _interval_delta(interval: str) -> timedelta:
    mapping = {
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
    }
    if interval not in mapping:
        raise ValueError(f"Unsupported crypto interval: {interval}")
    return mapping[interval]


def _fetch_symbol_history(
    provider: BinanceCCXTProvider | KrakenCCXTProvider | CoinbaseCCXTProvider,
    symbol: str,
    interval: str,
    *,
    start: datetime | None,
    total_limit: int,
    batch_limit: int,
) -> pl.DataFrame:
    remaining = max(1, total_limit)
    cursor = start
    frames: list[pl.DataFrame] = []
    step = _interval_delta(interval)
    while remaining > 0:
        limit = min(batch_limit, remaining)
        frame = provider.fetch_ohlcv(symbol, interval, start=cursor, limit=limit)
        if frame.is_empty():
            break
        frames.append(frame)
        if frame.height < limit:
            break
        cursor_value = frame.get_column("timestamp").max()
        cursor = _as_datetime(cursor_value)
        if cursor is None:
            break
        cursor = cursor.replace(tzinfo=timezone.utc) + step
        remaining -= frame.height
    if not frames:
        return pl.DataFrame(schema=CRYPTO_PARQUET_SCHEMA)
    return pl.concat(frames, how="vertical").unique(subset=["timestamp"], keep="last").sort("timestamp")


def crypto_show_universe_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    universe: str | None = None,
    symbols_override: list[str] | None = None,
) -> list[str]:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    return _resolve_symbols(cfg, crypto_cfg, universe=universe, symbols_override=symbols_override)


def crypto_diff_universe_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    left_universe: str,
    right_universe: str,
) -> dict[str, object]:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    left = set(_resolve_symbols(cfg, crypto_cfg, universe=left_universe, symbols_override=None))
    right = set(_resolve_symbols(cfg, crypto_cfg, universe=right_universe, symbols_override=None))
    return {
        "exchange": crypto_cfg.exchange,
        "market_type": crypto_cfg.market_type,
        "left_universe": left_universe,
        "right_universe": right_universe,
        "left_only": sorted(left - right),
        "right_only": sorted(right - left),
        "shared": sorted(left & right),
    }


def crypto_inspect_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    interval: str,
    universe: str | None = None,
    symbols_override: list[str] | None = None,
) -> list[dict[str, object]]:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    symbols = _resolve_symbols(cfg, crypto_cfg, universe=universe, symbols_override=symbols_override)
    out: list[dict[str, object]] = []
    for symbol in symbols:
        path = crypto_parquet_path(
            crypto_cfg.root,
            exchange=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            interval=interval,
            symbol=symbol,
        )
        frame = read_crypto_parquet(path)
        start = None
        end = None
        rows = 0
        if frame is not None and not frame.is_empty():
            rows = frame.height
            start = _as_datetime(frame.get_column("timestamp").min())
            end = _as_datetime(frame.get_column("timestamp").max())
        out.append(
            {
                "symbol": symbol,
                "path": str(path),
                "exists": path.exists(),
                "rows": rows,
                "start": start.isoformat() if start is not None else None,
                "end": end.isoformat() if end is not None else None,
            }
        )
    return out


def crypto_prune_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    interval: str,
    universe: str | None = None,
    symbols_override: list[str] | None = None,
    apply: bool = False,
) -> list[str]:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    if apply and symbols_override:
        raise ValueError("crypto prune with apply=True requires a resolved universe; symbols_override is not a complete keep set")
    keep = set(_resolve_symbols(cfg, crypto_cfg, universe=universe, symbols_override=symbols_override))
    root = crypto_cfg.root / crypto_cfg.exchange / crypto_cfg.market_type / interval
    if not root.exists():
        return []
    pruned: list[str] = []
    for path in sorted(root.glob("*.parquet")):
        if path.stem in keep:
            continue
        pruned.append(str(path))
        if apply:
            path.unlink(missing_ok=True)
    return pruned


def _as_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    return None


def _frames_equal(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    if left.height != right.height or left.columns != right.columns:
        return False
    try:
        return bool(left.equals(right))
    except Exception:
        return False


def crypto_refresh_universe_from_config(
    cfg: ConfigLike,
    *,
    exchange: str | None = None,
    provider_name: str | None = None,
    universe: str | None = None,
    limit: int | None = None,
) -> CryptoUniverseRefreshResult:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    selected_provider = provider_name or crypto_cfg.universe_refresh_provider
    if selected_provider != "coingecko":
        raise ValueError(f"Unsupported crypto universe refresh provider: {selected_provider}")
    universe_name = universe or "crypto_high_liquidity"
    market_provider = CoinGeckoProvider()
    exchange_provider = _provider_for(crypto_cfg)
    tradable_symbols = set(exchange_provider.list_symbols())

    metadata_entries = _fetch_dynamic_metadata(
        market_provider,
        crypto_cfg=crypto_cfg,
        tradable_symbols=tradable_symbols,
        universe_name=universe_name,
        limit=limit or crypto_cfg.universe_refresh_limit,
    )
    registry_path = merge_dynamic_registry(cfg, metadata_entries)
    universe_path = write_dynamic_universe(
        cfg,
        universe_name,
        [entry["symbol_canonical"] for entry in metadata_entries],
        metadata={
            "provider": selected_provider,
            "exchange": crypto_cfg.exchange,
            "market_type": crypto_cfg.market_type,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "candidates_seen": len(metadata_entries),
        },
    )
    return {
        "provider": selected_provider,
        "exchange": crypto_cfg.exchange,
        "market_type": crypto_cfg.market_type,
        "universe": universe_name,
        "registry_path": str(registry_path),
        "universe_path": str(universe_path),
        "candidates_seen": len(metadata_entries),
        "symbols_selected": [entry["symbol_canonical"] for entry in metadata_entries],
    }


def _fetch_dynamic_metadata(
    provider: CoinGeckoProvider,
    *,
    crypto_cfg: _CryptoConfig,
    tradable_symbols: set[str],
    universe_name: str,
    limit: int,
) -> list[CryptoMetadataEntry]:
    results: list[CryptoMetadataEntry] = []
    seen_symbols: set[str] = set()
    per_page = min(max(limit, 1), 250)
    for page in range(1, max(1, crypto_cfg.universe_refresh_pages) + 1):
        items = provider.fetch_markets(per_page=per_page, page=page)
        if not items:
            break
        for item in items:
            try:
                entry = _coingecko_item_to_metadata(item, crypto_cfg=crypto_cfg, universe_name=universe_name)
            except ValueError:
                # CoinGecko symbols occasionally contain malformed separators or extra parts.
                # Skip those candidates rather than aborting the entire refresh batch.
                continue
            if entry is None:
                continue
            if entry["symbol_canonical"] not in tradable_symbols:
                continue
            if entry["symbol_canonical"] in seen_symbols:
                continue
            seen_symbols.add(entry["symbol_canonical"])
            results.append(entry)
            if len(results) >= limit:
                return results
    return results


def _coingecko_item_to_metadata(
    item: dict[str, object],
    *,
    crypto_cfg: _CryptoConfig,
    universe_name: str,
) -> CryptoMetadataEntry | None:
    coingecko_id = str(item.get("id") or "").strip().lower()
    symbol = str(item.get("symbol") or "").strip().upper()
    name = str(item.get("name") or "").strip()
    if not coingecko_id or not symbol or not name:
        return None
    if coingecko_id in crypto_cfg.stablecoin_ids or coingecko_id in crypto_cfg.excluded_ids:
        return None
    if symbol in crypto_cfg.excluded_symbols:
        return None
    if crypto_cfg.exclude_wrapped_assets and _looks_wrapped_asset(coingecko_id, symbol, name):
        return None
    market_cap = _as_float(item.get("market_cap"))
    total_volume = _as_float(item.get("total_volume"))
    if market_cap is not None and market_cap < crypto_cfg.universe_refresh_min_market_cap:
        return None
    if total_volume is not None and total_volume < crypto_cfg.universe_refresh_min_volume:
        return None
    quote_asset = crypto_cfg.quote_assets[0]
    symbol_canonical = normalize_crypto_symbol(f"{symbol}_{quote_asset}")
    return {
        "coingecko_id": coingecko_id,
        "symbol_canonical": symbol_canonical,
        "source_symbol": f"{symbol}/{quote_asset}",
        "name": name,
        "base_asset": symbol,
        "quote_asset": quote_asset,
        "market_cap_rank": _as_int(item.get("market_cap_rank")),
        "market_cap": market_cap,
        "total_volume": total_volume,
        "exchange": crypto_cfg.exchange,
        "market_type": crypto_cfg.market_type,
        "is_active": True,
        "universe_tags": ["crypto", universe_name],
    }


def _looks_wrapped_asset(coingecko_id: str, symbol: str, name: str) -> bool:
    text = f"{coingecko_id} {symbol} {name}".lower()
    return text.startswith("wrapped ") or "wrapped-" in text or symbol.startswith("W") and "wrapped" in text


def _as_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    return None
