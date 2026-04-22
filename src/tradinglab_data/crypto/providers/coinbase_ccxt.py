from __future__ import annotations

from dataclasses import dataclass

from .ccxt_exchange import CCXTExchangeProvider


@dataclass
class CoinbaseCCXTProvider(CCXTExchangeProvider):
    exchange_name: str = "coinbase"
    market_type: str = "spot"
    quote_assets: tuple[str, ...] = ("USD", "USDT")
