from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

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


def test_load_symbols_from_config_prefers_override_list(dummy_cfg_factory):
    cfg = dummy_cfg_factory({})

    out = workflows._load_symbols_from_config(cfg, [" msft ", "AAPL", "MSFT", ""])

    assert out == ["AAPL", "MSFT"]


def test_symbol_and_currency_helpers_cover_none_nan_and_missing_currency():
    assert workflows._normalize_symbol(None) == ""
    assert workflows._normalize_symbol(float("nan")) == ""
    assert workflows._normalize_symbol(" aapl ") == "AAPL"
    assert workflows._normalized_currency_series(pl.DataFrame({"symbol": ["AAA"]})) is None


def test_load_symbols_from_config_raises_when_universe_empty(dummy_cfg_factory, monkeypatch):
    cfg = dummy_cfg_factory({"paths": {"store_root": "/tmp/store"}})
    monkeypatch.setattr(workflows, "load_universe_frame", lambda *args, **kwargs: pl.DataFrame({"symbol": []}))

    with pytest.raises(ValueError, match="No universe symbols available"):
        workflows._load_symbols_from_config(cfg)


def test_read_daily_close_returns_empty_for_missing_or_incomplete_file(tmp_path: Path):
    missing = workflows._read_daily_close(tmp_path, "MISSING")
    assert missing.is_empty()

    (tmp_path / "BAD.parquet").parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"date": [datetime(2026, 1, 1)]}).write_parquet(tmp_path / "BAD.parquet")
    bad = workflows._read_daily_close(tmp_path, "BAD")
    assert bad.is_empty()


def test_fetch_shares_full_handles_none_and_frame_build_failure(monkeypatch):
    class _TickerNone:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_shares_full(self, start=None, end=None):
            return None

    monkeypatch.setattr(workflows.yf, "Ticker", _TickerNone)
    assert workflows._fetch_shares_full("AAA").is_empty()


def test_fetch_shares_full_handles_frame_construction_failure(monkeypatch):
    class _BadShares:
        index = object()

        def to_numpy(self):
            raise RuntimeError("boom")

    class _Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_shares_full(self, start=None, end=None):
            return _BadShares()

    monkeypatch.setattr(workflows.yf, "Ticker", _Ticker)
    assert workflows._fetch_shares_full("AAA").is_empty()


def test_fetch_shares_full_success_and_fetch_sector_empty_info(monkeypatch):
    class _Shares:
        index = [datetime(2026, 1, 1), datetime(2026, 1, 1), datetime(2026, 2, 1)]

        def to_numpy(self):
            return [1_000_000.0, 1_100_000.0, 2_000_000.0]

    class _Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_shares_full(self, start=None, end=None):
            return _Shares()

        def get_info(self):
            return {}

    monkeypatch.setattr(workflows.yf, "Ticker", _Ticker)

    shares = workflows._fetch_shares_full("AAA")
    assert shares.height == 2
    assert shares.get_column("shares_outstanding").to_list() == [1_100_000.0, 2_000_000.0]
    assert workflows._fetch_sector("AAA") is None


def test_build_market_cap_frame_returns_empty_for_missing_inputs_and_non_usd_prices():
    empty = workflows.build_market_cap_frame("AAA", pl.DataFrame(), pl.DataFrame())
    assert empty.is_empty()

    non_usd = workflows.build_market_cap_frame(
        "AAA",
        pl.DataFrame({"date": [datetime(2026, 1, 30)], "close": [11.0], "currency": ["EUR"]}),
        pl.DataFrame({"date": [datetime(2026, 1, 1)], "shares_outstanding": [1_000_000.0]}),
    )
    assert non_usd.is_empty()

    no_shares = workflows.build_market_cap_frame(
        "AAA",
        pl.DataFrame({"date": [datetime(2026, 1, 30)], "close": [11.0], "currency": ["USD"]}),
        pl.DataFrame({"date": [datetime(2026, 1, 1)], "shares_outstanding": [None]}),
    )
    assert no_shares.is_empty()


def test_build_market_cap_frame_returns_empty_when_no_backward_share_match_exists():
    frame = workflows.build_market_cap_frame(
        "AAA",
        pl.DataFrame({"date": [datetime(2026, 1, 1)], "close": [11.0], "currency": ["USD"]}),
        pl.DataFrame({"date": [datetime(2026, 2, 1)], "shares_outstanding": [1_000_000.0]}),
    )
    assert frame.is_empty()


def test_sync_market_caps_yahoo_reports_non_usd_listing_currency(tmp_path: Path, monkeypatch, caplog: pytest.LogCaptureFixture):
    daily_root = tmp_path / "daily"
    cap_root = tmp_path / "market_caps"
    daily_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-30"],
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
            "close": [10.0, 11.0],
            "adj_close": [10.0, 11.0],
            "volume": [100.0, 110.0],
            "currency": ["EUR", "EUR"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(daily_root / "AAA.parquet")
    monkeypatch.setattr(
        workflows,
        "_fetch_shares_full",
        lambda symbol, start=None, end=None: pl.DataFrame({"date": [datetime(2026, 1, 1)], "shares_outstanding": [1_000_000.0]}),
    )
    caplog.set_level("WARNING")

    result = workflows.sync_market_caps_yahoo(["AAA"], daily_root=daily_root, market_cap_root=cap_root)

    assert result["symbols_written"] == 0
    assert result["skipped"]["AAA"] == "non_usd_listing_currency:EUR"
    assert "skipping market-cap sync for non-USD listing AAA: EUR" in caplog.text


def test_sync_market_caps_yahoo_filters_blank_symbols_and_reports_no_usd_or_shares(tmp_path: Path, monkeypatch):
    daily_root = tmp_path / "daily"
    cap_root = tmp_path / "market_caps"
    _write_daily(daily_root, "AAA")
    monkeypatch.setattr(workflows, "_fetch_shares_full", lambda symbol, start=None, end=None: pl.DataFrame())

    result = workflows.sync_market_caps_yahoo(["", "AAA"], daily_root=daily_root, market_cap_root=cap_root, end="2026-01-15")

    assert result["symbols_requested"] == 2
    assert result["symbols_written"] == 0
    assert result["skipped"]["AAA"] == "no_usd_price_or_shares"


def test_sync_sector_assignments_yahoo_writes_current_gics_sectors(tmp_path: Path, monkeypatch):
    output = tmp_path / "meta" / "sector_assignments.csv"
    monkeypatch.setattr(workflows, "_fetch_sector", lambda symbol: {"AAA": "Financials", "BAD": "Unknown Sector"}.get(symbol))

    result = workflows.sync_sector_assignments_yahoo(["AAA", "BAD", "MISS"], output_path=output)

    assert result["symbols_written"] == 1
    assert result["skipped"]["BAD"].startswith("unsupported_sector")
    frame = pl.read_csv(output)
    assert frame.get_column("sector").to_list() == ["Financials"]
    assert frame.get_column("effective_start").to_list() == [frame.get_column("ingested_at").item().split("T", 1)[0]]


def test_fetch_sector_handles_non_dict_and_blank_values(monkeypatch):
    class _TickerNonDict:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_info(self):
            return []

    class _TickerBlank:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_info(self):
            return {"sector": "   "}

    monkeypatch.setattr(workflows.yf, "Ticker", _TickerNonDict)
    assert workflows._fetch_sector("AAPL") is None

    monkeypatch.setattr(workflows.yf, "Ticker", _TickerBlank)
    assert workflows._fetch_sector("AAPL") is None


def test_fetch_sector_normalizes_yahoo_sector_to_gics(monkeypatch):
    class _Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_info(self):
            return {"sector": "Technology"}

    monkeypatch.setattr(workflows.yf, "Ticker", _Ticker)

    assert workflows._fetch_sector("AAPL") == "Information Technology"


def test_fetch_sector_returns_none_on_provider_failure(monkeypatch):
    class _Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_info(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(workflows.yf, "Ticker", _Ticker)

    assert workflows._fetch_sector("AAPL") is None


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


def test_download_index_level_returns_empty_on_no_data(monkeypatch):
    monkeypatch.setattr(workflows.yf, "download", lambda *args, **kwargs: None)
    frame = workflows._download_index_level("^SP500TR")
    assert frame.is_empty()
    assert frame.columns == ["date", "adj_close", "close"]


def test_download_index_level_normalizes_non_empty_response(monkeypatch):
    monkeypatch.setattr(
        workflows.yf,
        "download",
        lambda *args, **kwargs: pl.DataFrame(
            {"date": [datetime(2026, 1, 2)], "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "adj_close": [1.4], "volume": [100.0]}
        ).to_pandas(),
    )
    frame = workflows._download_index_level("^SP500TR")
    assert frame.height == 1
    assert "adj_close" in frame.columns


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


def test_build_index_return_frame_handles_empty_frame_and_price_fallback():
    empty = workflows.build_index_return_frame("SPX", pl.DataFrame(), source_symbol="SPXTR")
    assert empty.is_empty()

    frame = workflows.build_index_return_frame(
        "SPX",
        pl.DataFrame({"date": ["2026-01-02", "2026-01-05"], "close": [100.0, 101.0]}).with_columns(
            pl.col("date").str.strptime(pl.Datetime, strict=False)
        ),
        source_symbol="SPX",
        allow_price_fallback=True,
    )
    assert frame.height == 2


def test_build_index_return_frame_rejects_unsupported_or_missing_total_return():
    with pytest.raises(ValueError, match="Unsupported index_id"):
        workflows.build_index_return_frame("BAD", pl.DataFrame(), source_symbol="BAD")

    with pytest.raises(ValueError, match="No total-return level available"):
        workflows.build_index_return_frame(
            "SPX",
            pl.DataFrame({"date": ["2026-01-02"], "close": [100.0]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
            source_symbol="SPX",
            allow_price_fallback=False,
        )


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


def test_sync_sector_assignments_skips_blank_symbols_and_sync_index_returns_reports_empty_download(tmp_path: Path, monkeypatch):
    output = tmp_path / "meta" / "sector_assignments.csv"
    monkeypatch.setattr(workflows, "_fetch_sector", lambda symbol: "Financials")
    result = workflows.sync_sector_assignments_yahoo(["", "AAA"], output_path=output)
    assert result["symbols_written"] == 1

    monkeypatch.setattr(workflows, "_download_index_level", lambda *args, **kwargs: pl.DataFrame())
    index_result = workflows.sync_index_returns_yahoo(["SPX"], root=tmp_path / "index_returns")
    assert index_result["skipped"]["SPX"] == "empty_download"


def test_inspect_market_data_reports_missing_files(tmp_path: Path, dummy_cfg_factory):
    store = tmp_path / "store"
    meta = store / "meta"
    universe = meta / "universe_master.csv"
    universe.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["AAA"], "active": [1]}).write_csv(universe)
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "store_root": store,
                "universe_csv": universe,
                "market_cap_root": store / "parquet" / "market_caps",
                "sector_assignments_csv": meta / "sector_assignments.csv",
                "index_returns_root": store / "parquet" / "index_returns",
                "runs_root": store / "runs",
            }
        }
    )

    rows = workflows.inspect_market_data_from_config(cfg, index_ids=["SPX"])

    assert {"artifact": "market_cap", "id": "AAA", "exists": False, "rows": 0, "path": str(store / "parquet" / "market_caps" / "AAA.parquet")} in rows
    assert {"artifact": "index_return", "id": "SPX", "exists": False, "rows": 0, "path": str(store / "parquet" / "index_returns" / "SPX.parquet")} in rows


def test_validate_market_data_helpers_report_invalid_files(tmp_path: Path):
    market_root = tmp_path / "market_caps"
    index_root = tmp_path / "index_returns"
    sector_path = tmp_path / "meta" / "sector_assignments.csv"
    market_root.mkdir(parents=True, exist_ok=True)
    index_root.mkdir(parents=True, exist_ok=True)
    sector_path.parent.mkdir(parents=True, exist_ok=True)

    pl.DataFrame({"symbol": ["AAA"]}).write_parquet(market_root / "AAA.parquet")
    pl.DataFrame({"index_id": ["SPX"]}).write_parquet(index_root / "SPX.parquet")
    pl.DataFrame({"symbol": ["AAA"]}).write_csv(sector_path)

    market = workflows.validate_market_cap_store(market_root, ["AAA"])
    index = workflows.validate_index_return_store(index_root, ["SPX"])
    sector = workflows.validate_sector_assignment_file(sector_path)

    assert market["ok"] is False and market["errors"]
    assert index["ok"] is False and index["errors"]
    assert sector["ok"] is False and sector["errors"]


def test_sync_index_returns_yahoo_reports_builder_errors(tmp_path: Path, monkeypatch):
    root = tmp_path / "index_returns"
    monkeypatch.setattr(workflows, "_download_index_level", lambda *args, **kwargs: pl.DataFrame({"date": ["2026-01-02"]}))

    result = workflows.sync_index_returns_yahoo(["SPX"], root=root)

    assert result["index_ids_written"] == 0
    assert "No total-return level available" in result["skipped"]["SPX"]
