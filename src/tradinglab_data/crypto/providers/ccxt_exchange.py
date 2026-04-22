from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from ...provider_base import MarketDataProvider

SUPPORTED_INTERVALS = {"1d", "1h", "15m"}
INTERVAL_TO_DELTA = {
    "1d": timedelta(days=1),
    "1h": timedelta(hours=1),
    "15m": timedelta(minutes=15),
}


def _import_ccxt() -> Any:
    try:
        import ccxt  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("ccxt is required for crypto provider workflows") from exc
    return ccxt


def normalize_canonical_symbol(symbol: str) -> tuple[str, str]:
    raw = str(symbol or "").strip().upper().replace("-", "_").replace("/", "_")
    parts = [part for part in raw.split("_") if part]
    if len(parts) != 2:
        raise ValueError(f"Invalid crypto symbol: {symbol}")
    return parts[0], parts[1]


def _normalize_ohlcv_rows(
    rows: list[list[Any]],
    *,
    symbol: str,
    exchange: str,
    market_type: str,
    interval: str,
    provider: str,
    base_asset: str,
    quote_asset: str,
    source_symbol: str,
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "provider": pl.String,
                "exchange": pl.String,
                "market_type": pl.String,
                "symbol": pl.String,
                "base_asset": pl.String,
                "quote_asset": pl.String,
                "interval": pl.String,
                "is_closed": pl.Boolean,
                "ingested_at": pl.Datetime,
                "source_symbol": pl.String,
            }
        )
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    frame = pl.DataFrame(
        rows,
        schema=["timestamp_ms", "open", "high", "low", "close", "volume"],
        orient="row",
    ).with_columns(
        pl.from_epoch("timestamp_ms", time_unit="ms").alias("timestamp"),
        pl.lit(provider).alias("provider"),
        pl.lit(exchange).alias("exchange"),
        pl.lit(market_type).alias("market_type"),
        pl.lit(symbol).alias("symbol"),
        pl.lit(base_asset).alias("base_asset"),
        pl.lit(quote_asset).alias("quote_asset"),
        pl.lit(interval).alias("interval"),
        pl.lit(True).alias("is_closed"),
        pl.lit(ingested_at).alias("ingested_at"),
        pl.lit(source_symbol).alias("source_symbol"),
    )
    return frame.select(
        [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "provider",
            "exchange",
            "market_type",
            "symbol",
            "base_asset",
            "quote_asset",
            "interval",
            "is_closed",
            "ingested_at",
            "source_symbol",
        ]
    )


@dataclass
class CCXTExchangeProvider(MarketDataProvider):
    exchange_name: str
    market_type: str = "spot"
    quote_assets: tuple[str, ...] = ("USDT",)
    _client: Any | None = None

    def _client_or_create(self) -> Any:
        if self._client is not None:
            return self._client
        ccxt = _import_ccxt()
        exchange_ctor = getattr(ccxt, self.exchange_name, None)
        if exchange_ctor is None:
            raise ValueError(f"Unsupported ccxt exchange: {self.exchange_name}")
        self._client = exchange_ctor({"enableRateLimit": True, "options": {"defaultType": self.market_type}})
        return self._client

    def supports_interval(self, interval: str) -> bool:
        return interval in SUPPORTED_INTERVALS

    def normalize_symbol(self, symbol: str) -> str:
        raw = str(symbol or "").strip().upper().replace("-", "/").replace("_", "/")
        if "/" not in raw:
            raise ValueError(f"Crypto symbols must include a quote asset: {symbol}")
        base_asset, quote_asset = raw.split("/", 1)
        return f"{base_asset.strip()}_{quote_asset.strip()}"

    def list_markets(self) -> list[dict[str, Any]]:
        client = self._client_or_create()
        markets = client.load_markets()
        return [market for market in markets.values() if isinstance(market, dict)]

    def list_symbols(self) -> list[str]:
        symbols = []
        for market in self.list_markets():
            if not market.get("active", True):
                continue
            if not market.get("spot", False):
                continue
            quote_asset = str(market.get("quote") or "").upper()
            if self.quote_assets and quote_asset not in self.quote_assets:
                continue
            symbols.append(self.normalize_symbol(str(market["symbol"])))
        return sorted(set(symbols))

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        if not self.supports_interval(interval):
            raise ValueError(f"Unsupported crypto interval: {interval}")
        base_asset, quote_asset = normalize_canonical_symbol(symbol)
        source_symbol = f"{base_asset}/{quote_asset}"
        client = self._client_or_create()
        since = None
        if start is not None:
            since = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000)
        rows = client.fetch_ohlcv(source_symbol, timeframe=interval, since=since, limit=limit)
        frame = _normalize_ohlcv_rows(
            rows,
            symbol=f"{base_asset}_{quote_asset}",
            exchange=self.exchange_name,
            market_type=self.market_type,
            interval=interval,
            provider="ccxt",
            base_asset=base_asset,
            quote_asset=quote_asset,
            source_symbol=source_symbol,
        )
        if end is not None and not frame.is_empty():
            end_naive = end.replace(tzinfo=timezone.utc).replace(tzinfo=None)
            frame = frame.filter(pl.col("timestamp") < end_naive)
        return frame
