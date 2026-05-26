from __future__ import annotations

from dataclasses import dataclass

DEFAULT_EXCHANGE_TIMEZONE = "America/New_York"


@dataclass(frozen=True)
class MarketSessionSpec:
    timezone_name: str
    regular_open_hour: int
    regular_open_minute: int
    regular_close_hour: int
    regular_close_minute: int
    post_close_hour: int
    post_close_minute: int


_US_SESSION_SPEC = MarketSessionSpec("America/New_York", 9, 30, 16, 0, 20, 0)
_EU_SESSION_SPEC = MarketSessionSpec("Europe/Berlin", 9, 0, 17, 30, 20, 0)
_LSE_SESSION_SPEC = MarketSessionSpec("Europe/London", 8, 0, 16, 30, 20, 0)

_SUFFIX_TO_SESSION_SPEC: tuple[tuple[str, MarketSessionSpec], ...] = (
    (".VI", MarketSessionSpec("Europe/Vienna", 9, 0, 17, 30, 20, 0)),
    (".DE", _EU_SESSION_SPEC),
    (".PA", MarketSessionSpec("Europe/Paris", 9, 0, 17, 30, 20, 0)),
    (".AS", MarketSessionSpec("Europe/Amsterdam", 9, 0, 17, 30, 20, 0)),
    (".BR", MarketSessionSpec("Europe/Brussels", 9, 0, 17, 30, 20, 0)),
    (".MI", MarketSessionSpec("Europe/Rome", 9, 0, 17, 30, 20, 0)),
    (".LS", MarketSessionSpec("Europe/Lisbon", 8, 0, 16, 30, 20, 0)),
    (".MC", MarketSessionSpec("Europe/Madrid", 9, 0, 17, 30, 20, 0)),
    (".HE", MarketSessionSpec("Europe/Helsinki", 10, 0, 18, 30, 20, 0)),
    (".ST", MarketSessionSpec("Europe/Stockholm", 9, 0, 17, 30, 20, 0)),
    (".CO", MarketSessionSpec("Europe/Copenhagen", 9, 0, 17, 30, 20, 0)),
    (".OL", MarketSessionSpec("Europe/Oslo", 9, 0, 17, 30, 20, 0)),
    (".SW", MarketSessionSpec("Europe/Zurich", 9, 0, 17, 30, 20, 0)),
    (".L", _LSE_SESSION_SPEC),
)


def resolve_exchange_timezone_for_symbol(symbol: str, *, default_timezone: str = DEFAULT_EXCHANGE_TIMEZONE) -> str:
    return resolve_market_session_spec_for_symbol(symbol, default_timezone=default_timezone).timezone_name


def resolve_market_session_spec_for_symbol(
    symbol: str,
    *,
    default_timezone: str = DEFAULT_EXCHANGE_TIMEZONE,
) -> MarketSessionSpec:
    raw = str(symbol).strip().upper()
    for suffix, spec in _SUFFIX_TO_SESSION_SPEC:
        if raw.endswith(suffix):
            return spec
    return MarketSessionSpec(default_timezone, 9, 30, 16, 0, 20, 0)
