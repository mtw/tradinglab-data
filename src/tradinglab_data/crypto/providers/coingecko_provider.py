from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests  # type: ignore[import-untyped]


@dataclass(frozen=True)
class CoinGeckoProvider:
    base_url: str = "https://api.coingecko.com/api/v3"
    timeout_seconds: float = 20.0

    def fetch_markets(
        self,
        *,
        vs_currency: str = "usd",
        per_page: int = 100,
        page: int = 1,
        order: str = "market_cap_desc",
        sparkline: bool = False,
    ) -> list[dict[str, Any]]:
        response = requests.get(
            f"{self.base_url}/coins/markets",
            params={
                "vs_currency": vs_currency,
                "order": order,
                "per_page": per_page,
                "page": page,
                "sparkline": str(sparkline).lower(),
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("CoinGecko markets response must be a list")
        return [item for item in payload if isinstance(item, dict)]
