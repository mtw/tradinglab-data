from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from ..config import ConfigLike, crypto_registry_path, crypto_universe_dir_path
from ..contracts import CryptoMetadataEntry, CryptoRegistryEntry
from .symbols import normalize_crypto_symbol

STATIC_CRYPTO_REGISTRY: tuple[CryptoRegistryEntry, ...] = (
    {"symbol_canonical": "BTC_USDT", "source_symbol": "BTC/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "BTC", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_majors", "crypto_core"]},
    {"symbol_canonical": "ETH_USDT", "source_symbol": "ETH/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "ETH", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_majors", "crypto_core"]},
    {"symbol_canonical": "SOL_USDT", "source_symbol": "SOL/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "SOL", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_majors", "crypto_core"]},
    {"symbol_canonical": "XRP_USDT", "source_symbol": "XRP/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "XRP", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_majors", "crypto_core"]},
    {"symbol_canonical": "BNB_USDT", "source_symbol": "BNB/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "BNB", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_majors", "crypto_core"]},
    {"symbol_canonical": "ADA_USDT", "source_symbol": "ADA/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "ADA", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "DOGE_USDT", "source_symbol": "DOGE/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "DOGE", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "LINK_USDT", "source_symbol": "LINK/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "LINK", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "AVAX_USDT", "source_symbol": "AVAX/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "AVAX", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "DOT_USDT", "source_symbol": "DOT/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "DOT", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "LTC_USDT", "source_symbol": "LTC/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "LTC", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "BCH_USDT", "source_symbol": "BCH/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "BCH", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "TRX_USDT", "source_symbol": "TRX/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "TRX", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "TON_USDT", "source_symbol": "TON/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "TON", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "NEAR_USDT", "source_symbol": "NEAR/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "NEAR", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "APT_USDT", "source_symbol": "APT/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "APT", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
    {"symbol_canonical": "SUI_USDT", "source_symbol": "SUI/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "SUI", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto", "crypto_core", "crypto_high_liquidity"]},
)

STATIC_CRYPTO_UNIVERSES = {
    "crypto_majors": ("BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "BNB_USDT"),
    "crypto_core": tuple(entry["symbol_canonical"] for entry in STATIC_CRYPTO_REGISTRY if "crypto_core" in entry["universe_tags"]),
    "crypto_high_liquidity": tuple(
        entry["symbol_canonical"] for entry in STATIC_CRYPTO_REGISTRY if "crypto_high_liquidity" in entry["universe_tags"]
    ),
}


def _load_json(path: Path) -> object | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _dynamic_registry_entries(cfg: ConfigLike | None = None) -> list[CryptoRegistryEntry]:
    if cfg is None:
        return []
    payload = _load_json(crypto_registry_path(cfg))
    if not isinstance(payload, list):
        return []
    entries: list[CryptoRegistryEntry] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = normalize_crypto_symbol(str(item.get("symbol_canonical") or ""))
        if not symbol:
            continue
        entries.append(
            {
                "symbol_canonical": symbol,
                "source_symbol": str(item.get("source_symbol") or ""),
                "exchange": str(item.get("exchange") or "binance"),
                "market_type": str(item.get("market_type") or "spot"),
                "base_asset": str(item.get("base_asset") or symbol.split("_", 1)[0]),
                "quote_asset": str(item.get("quote_asset") or symbol.split("_", 1)[1]),
                "is_active": bool(item.get("is_active", True)),
                "universe_tags": [str(tag) for tag in item.get("universe_tags", [])],
            }
        )
    return entries


def load_crypto_registry(
    *,
    cfg: ConfigLike | None = None,
    exchange: str = "binance",
    market_type: str = "spot",
    quote_assets: tuple[str, ...] = ("USDT",),
) -> list[CryptoRegistryEntry]:
    allowed_quotes = {asset.upper() for asset in quote_assets}
    merged: dict[str, CryptoRegistryEntry] = {
        entry["symbol_canonical"]: entry
        for entry in STATIC_CRYPTO_REGISTRY
        if entry["exchange"] == exchange and entry["market_type"] == market_type and entry["quote_asset"] in allowed_quotes
    }
    for entry in _dynamic_registry_entries(cfg):
        if entry["exchange"] != exchange or entry["market_type"] != market_type or entry["quote_asset"] not in allowed_quotes:
            continue
        merged[entry["symbol_canonical"]] = entry
    return sorted(merged.values(), key=lambda item: item["symbol_canonical"])


def _dynamic_universes(cfg: ConfigLike | None = None) -> dict[str, tuple[str, ...]]:
    if cfg is None:
        return {}
    root = crypto_universe_dir_path(cfg)
    if not root.exists() or not root.is_dir():
        return {}
    out: dict[str, tuple[str, ...]] = {}
    for path in sorted(root.glob("*.json")):
        payload = _load_json(path)
        if not isinstance(payload, dict):
            continue
        symbols = payload.get("symbols")
        if not isinstance(symbols, list):
            continue
        normalized = tuple(normalize_crypto_symbol(str(symbol)) for symbol in symbols if str(symbol).strip())
        if normalized:
            out[path.stem] = normalized
    return out


def load_crypto_universes(cfg: ConfigLike | None = None) -> dict[str, tuple[str, ...]]:
    universes = dict(STATIC_CRYPTO_UNIVERSES)
    universes.update(_dynamic_universes(cfg))
    return universes


def resolve_crypto_universe(
    universe: str,
    *,
    cfg: ConfigLike | None = None,
    exchange: str = "binance",
    market_type: str = "spot",
    quote_assets: tuple[str, ...] = ("USDT",),
) -> list[CryptoRegistryEntry]:
    registry = {
        entry["symbol_canonical"]: entry
        for entry in load_crypto_registry(cfg=cfg, exchange=exchange, market_type=market_type, quote_assets=quote_assets)
    }
    symbols = load_crypto_universes(cfg).get(universe)
    if symbols is None:
        raise ValueError(f"Unknown crypto universe: {universe}")
    return [registry[normalize_crypto_symbol(symbol)] for symbol in symbols if normalize_crypto_symbol(symbol) in registry]


def write_dynamic_registry(cfg: ConfigLike, entries: list[CryptoMetadataEntry]) -> Path:
    out_path = crypto_registry_path(cfg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    return out_path


def merge_dynamic_registry(cfg: ConfigLike, entries: list[CryptoMetadataEntry]) -> Path:
    out_path = crypto_registry_path(cfg)
    payload = _load_json(out_path)
    preserved: list[object] = []
    merged: dict[tuple[str, str, str], object] = {}
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                preserved.append(item)
                continue
            key = _registry_entry_key(item)
            if key is None:
                preserved.append(item)
                continue
            merged[key] = item
    for entry in entries:
        key = _registry_entry_key(entry)
        if key is not None:
            merged[key] = entry
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps([*preserved, *merged.values()], indent=2), encoding="utf-8")
    return out_path


def _registry_entry_key(entry: Mapping[str, object]) -> tuple[str, str, str] | None:
    try:
        symbol = normalize_crypto_symbol(str(entry.get("symbol_canonical") or ""))
    except ValueError:
        return None
    exchange = str(entry.get("exchange") or "binance")
    market_type = str(entry.get("market_type") or "spot")
    return (symbol, exchange, market_type)


def write_dynamic_universe(cfg: ConfigLike, universe: str, symbols: list[str], metadata: dict[str, object]) -> Path:
    out_dir = crypto_universe_dir_path(cfg)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{universe}.json"
    payload = {
        "universe": universe,
        "symbols": symbols,
        **metadata,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


CRYPTO_UNIVERSES = STATIC_CRYPTO_UNIVERSES
