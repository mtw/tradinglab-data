from __future__ import annotations

from pathlib import Path

import polars as pl

from tradinglab_data.universe import load_universe, load_universe_frame


def test_load_universe_frame_filters_active_and_bad_symbols(tmp_path: Path):
    p = tmp_path / "universe.csv"
    pl.DataFrame({"symbol": [" AAPL ", "BAD$SYM", "", "MSFT"], "active": [1, 1, 1, 0]}).write_csv(str(p))
    df = load_universe_frame(p)
    assert df.get_column("symbol").to_list() == ["AAPL"]


def test_load_universe_raises_when_symbol_column_missing(tmp_path: Path):
    p = tmp_path / "universe.csv"
    pl.DataFrame({"name": ["x"]}).write_csv(str(p))
    try:
        load_universe_frame(p)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "symbol" in str(e)


def test_load_universe_returns_symbols(tmp_path: Path):
    p = tmp_path / "universe.csv"
    pl.DataFrame({"symbol": ["AAPL", "MSFT"], "active": [1, 1]}).write_csv(str(p))
    assert load_universe(p) == ["AAPL", "MSFT"]


def test_load_universe_frame_applies_ticker_overrides(tmp_path: Path):
    p = tmp_path / "universe.csv"
    ov = tmp_path / "ticker_overrides.csv"
    pl.DataFrame({"symbol": ["BTEK.L", "AAPL"], "active": [1, 1]}).write_csv(str(p))
    ov.write_text("raw,yahoo\nBTEK.L,2B70.DE\n", encoding="utf-8")
    df = load_universe_frame(p, ticker_overrides_path=ov)
    assert df.get_column("symbol").to_list() == ["2B70.DE", "AAPL"]
