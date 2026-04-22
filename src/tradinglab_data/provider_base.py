from __future__ import annotations

from datetime import datetime
from typing import Protocol

import polars as pl


class MarketDataProvider(Protocol):
    def list_symbols(self) -> list[str]: ...

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame: ...

    def normalize_symbol(self, symbol: str) -> str: ...
