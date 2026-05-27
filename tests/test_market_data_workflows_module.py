from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

import tradinglab_data.market_data_workflows as workflows


def _write_daily(root: Path, symbol: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-30", "2026-02-27"],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.0, 11.0, 12.0],
            "adj_close": [10.0, 11.0, 12.0],
            "volume": [100.0, 110.0, 120.0],
            "currency": ["USD", "USD", "USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(root / f"{symbol}.parquet")


def test_sync_market_caps_yahoo_writes_monthly_market_cap(tmp_path: Path, monkeypatch):
    daily_root = tmp_path / "daily"
    cap_root = tmp_path / "market_caps"
    _write_daily(daily_root, "AAA")
    monkeypatch.setattr(
        workflows,
        "_fetch_shares_full",
        lambda symbol, start=None, end=None: pl.DataFrame(
            {"date": [datetime(2026, 1, 1), datetime(2026, 2, 1)], "shares_outstanding": [1_000_000.0, 2_000_000.0]}
        ),
    )

    result = workflows.sync_market_caps_yahoo(["AAA"], daily_root=daily_root, market_cap_root=cap_root)

    assert result["symbols_written"] == 1
    frame = pl.read_parquet(cap_root / "AAA.parquet")
    assert frame.height == 2
    assert frame.get_column("market_cap_usd_millions").to_list() == [11.0, 24.0]


def test_sync_market_caps_uses_prior_shares_when_start_filters_prices(tmp_path: Path, monkeypatch):
    daily_root = tmp_path / "daily"
    cap_root = tmp_path / "market_caps"
    _write_daily(daily_root, "AAA")
    calls: list[dict[str, str | None]] = []

    def _shares(symbol, start=None, end=None):
        calls.append({"start": start, "end": end})
        return pl.DataFrame({"date": [datetime(2026, 1, 1)], "shares_outstanding": [1_000_000.0]})

    monkeypatch.setattr(workflows, "_fetch_shares_full", _shares)

    result = workflows.sync_market_caps_yahoo(["AAA"], daily_root=daily_root, market_cap_root=cap_root, start="2026-01-15")

    assert result["symbols_written"] == 1
    assert calls == [{"start": None, "end": None}]
    frame = pl.read_parquet(cap_root / "AAA.parquet")
    assert frame.get_column("market_cap_usd_millions").to_list() == [11.0, 12.0]


def test_build_market_cap_frame_normalizes_mixed_datetime_units():
    frame = workflows.build_market_cap_frame(
        "AAA",
        pl.DataFrame(
            {
                "date": [datetime(2026, 1, 30), datetime(2026, 2, 27)],
                "close": [11.0, 12.0],
                "currency": ["USD", "USD"],
            }
        ),
        pl.DataFrame({"date": [datetime(2026, 1, 1), datetime(2026, 2, 1)], "shares_outstanding": [1_000_000.0, 2_000_000.0]}),
        provider="fixture",
    )

    assert frame.get_column("market_cap_usd_millions").to_list() == [11.0, 24.0]


def test_sync_sector_assignments_yahoo_writes_current_gics_sectors(tmp_path: Path, monkeypatch):
    output = tmp_path / "meta" / "sector_assignments.csv"
    monkeypatch.setattr(workflows, "_fetch_sector", lambda symbol: {"AAA": "Financials", "BAD": "Unknown Sector"}.get(symbol))

    result = workflows.sync_sector_assignments_yahoo(["AAA", "BAD", "MISS"], output_path=output)

    assert result["symbols_written"] == 1
    assert result["skipped"]["BAD"].startswith("unsupported_sector")
    frame = pl.read_csv(output)
    assert frame.get_column("sector").to_list() == ["Financials"]


def test_fetch_sector_normalizes_yahoo_sector_to_gics(monkeypatch):
    class _Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_info(self):
            return {"sector": "Technology"}

    monkeypatch.setattr(workflows.yf, "Ticker", _Ticker)

    assert workflows._fetch_sector("AAPL") == "Information Technology"


def test_sync_index_returns_yahoo_writes_total_return_frame(tmp_path: Path, monkeypatch):
    root = tmp_path / "index_returns"
    price_frame = pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "adj_close": [1000.0, 1010.0, 1000.0],
            "close": [1000.0, 1010.0, 1000.0],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))
    monkeypatch.setattr(workflows, "_download_index_level", lambda source_symbol, start=None, end=None: price_frame)

    result = workflows.sync_index_returns_yahoo(["SPX", "BAD"], root=root)

    assert result["index_ids_written"] == 1
    assert result["skipped"]["BAD"] == "unsupported_index_id"
    frame = pl.read_parquet(root / "SPX.parquet")
    assert frame.get_column("return").to_list()[1] == 0.01


def test_index_provider_symbols_use_yahoo_total_return_series():
    assert workflows.INDEX_PROVIDER_SYMBOLS == {"SPX": "^SP500TR", "RTY": "^RUTTR", "NDX": "^NDXT"}


def test_build_index_return_frame_drops_null_total_return_levels_before_pct_change():
    frame = workflows.build_index_return_frame(
        "SPX",
        pl.DataFrame(
            {
                "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
                "adj_close": [100.0, None, 110.0],
                "close": [100.0, 105.0, 110.0],
            }
        ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
        source_symbol="SPXTR",
    )

    assert frame.height == 2
    assert frame.get_column("return").to_list() == [None, 0.1]


def test_sync_market_data_index_only_does_not_require_universe(tmp_path: Path, dummy_cfg_factory, monkeypatch):
    store = tmp_path / "store"
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "store_root": store,
                "universe_csv": store / "meta" / "missing_universe.csv",
                "parquet_root": store / "parquet" / "daily",
                "index_returns_root": store / "parquet" / "index_returns",
                "runs_root": store / "runs",
            }
        }
    )
    calls: list[dict[str, object]] = []

    def _sync_index_returns(index_ids, *, root, start=None, end=None, allow_price_fallback=False):
        calls.append(
            {
                "index_ids": list(index_ids),
                "root": Path(root),
                "start": start,
                "end": end,
                "allow_price_fallback": allow_price_fallback,
            }
        )
        return {"ok": True, "index_ids_written": len(index_ids), "written": list(index_ids), "skipped": {}, "root": str(root)}

    monkeypatch.setattr(workflows, "sync_index_returns_yahoo", _sync_index_returns)

    result = workflows.sync_market_data_from_config(
        cfg,
        index_ids=["SPX"],
        include_market_caps=False,
        include_sectors=False,
        include_index_returns=True,
    )

    assert result["index_returns"]["ok"] is True
    assert calls == [
        {
            "index_ids": ["SPX"],
            "root": store / "parquet" / "index_returns",
            "start": None,
            "end": None,
            "allow_price_fallback": False,
        }
    ]


def test_validate_and_inspect_market_data_from_config(tmp_path: Path, dummy_cfg_factory, monkeypatch):
    store = tmp_path / "store"
    daily_root = store / "parquet" / "daily"
    market_root = store / "parquet" / "market_caps"
    index_root = store / "parquet" / "index_returns"
    meta = store / "meta"
    universe = meta / "universe_master.csv"
    universe.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["AAA"], "active": [1]}).write_csv(universe)
    _write_daily(daily_root, "AAA")
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "store_root": store,
                "universe_csv": universe,
                "parquet_root": daily_root,
                "market_cap_root": market_root,
                "sector_assignments_csv": meta / "sector_assignments.csv",
                "index_returns_root": index_root,
                "runs_root": store / "runs",
            }
        }
    )
    monkeypatch.setattr(
        workflows,
        "_fetch_shares_full",
        lambda symbol, start=None, end=None: pl.DataFrame({"date": [datetime(2026, 1, 1)], "shares_outstanding": [1_000_000.0]}),
    )
    monkeypatch.setattr(workflows, "_fetch_sector", lambda symbol: "Information Technology")
    monkeypatch.setattr(
        workflows,
        "_download_index_level",
        lambda source_symbol, start=None, end=None: pl.DataFrame(
            {"date": ["2026-01-02", "2026-01-05"], "adj_close": [100.0, 101.0], "close": [100.0, 101.0]}
        ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
    )

    workflows.sync_market_data_from_config(cfg, index_ids=["SPX"])
    validation = workflows.validate_market_data_from_config(cfg, index_ids=["SPX"])
    inspection = workflows.inspect_market_data_from_config(cfg, index_ids=["SPX"])

    assert validation["ok"] is True
    assert any(item["artifact"] == "market_cap" and item["exists"] for item in inspection)
