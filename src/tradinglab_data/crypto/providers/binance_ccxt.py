from __future__ import annotations

from dataclasses import dataclass

from .ccxt_exchange import CCXTExchangeProvider
from .ccxt_exchange import _normalize_ohlcv_rows as _normalize_ohlcv_rows
from .ccxt_exchange import normalize_canonical_symbol as normalize_canonical_symbol


@dataclass
class BinanceCCXTProvider(CCXTExchangeProvider):
    exchange_name: str = "binance"
    market_type: str = "spot"
    quote_assets: tuple[str, ...] = ("USDT",)
