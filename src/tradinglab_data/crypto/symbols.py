from __future__ import annotations


def normalize_crypto_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper().replace("-", "_").replace("/", "_")
    parts = [part for part in raw.split("_") if part]
    if len(parts) != 2:
        raise ValueError(f"Invalid crypto symbol: {symbol}")
    return f"{parts[0]}_{parts[1]}"


def split_crypto_symbol(symbol: str) -> tuple[str, str]:
    normalized = normalize_crypto_symbol(symbol)
    base_asset, quote_asset = normalized.split("_", 1)
    return base_asset, quote_asset


def to_source_symbol(symbol: str) -> str:
    base_asset, quote_asset = split_crypto_symbol(symbol)
    return f"{base_asset}/{quote_asset}"
