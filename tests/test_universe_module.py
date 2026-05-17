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


def test_load_ticker_overrides_handles_missing_empty_bad_and_case(tmp_path: Path, capsys):
    from tradinglab_data.universe import canonicalize_symbol, load_ticker_overrides

    assert load_ticker_overrides(None) == {}
    assert load_ticker_overrides(tmp_path / "missing.csv") == {}
    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    assert load_ticker_overrides(empty) == {}

    malformed = tmp_path / "malformed.csv"
    malformed.write_text('"unterminated\n', encoding="utf-8")
    assert load_ticker_overrides(malformed) == {}

    wrong_columns = tmp_path / "wrong.csv"
    wrong_columns.write_text("source,target\nA,B\n", encoding="utf-8")
    assert load_ticker_overrides(wrong_columns) == {}

    overrides = tmp_path / "overrides.csv"
    overrides.write_text("Raw,Yahoo\n brk.b , brk-b \n,\n", encoding="utf-8")
    assert load_ticker_overrides(overrides) == {"BRK.B": "BRK-B"}
    assert canonicalize_symbol("", {"A": "B"}) == ""
    assert canonicalize_symbol(" brk.b ", {"BRK.B": "BRK-B"}) == "BRK-B"


def test_load_universe_frame_concats_shards_with_different_columns(tmp_path: Path):
    missing_main = tmp_path / "missing.csv"
    shard_dir = tmp_path / "shards"
    shard_dir.mkdir()
    (shard_dir / "a.csv").write_text("symbol,active,name\nAAA,1,Acme\nBAD SYM,1,Bad\n", encoding="utf-8")
    (shard_dir / "b.csv").write_text("symbol,active,exchange\nBBB,1,NASDAQ\n", encoding="utf-8")

    df = load_universe_frame(missing_main, universe_dir=shard_dir)

    assert df.get_column("symbol").to_list() == ["AAA", "BBB"]
    assert "name" in df.columns
    assert "exchange" in df.columns
