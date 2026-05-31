from __future__ import annotations

from pathlib import Path

import polars as pl

import tradinglab_data.ticker_map as ticker_map


def test_normalize_to_yahoo_by_exchange_country(monkeypatch):
    monkeypatch.setattr(ticker_map, "_OVERRIDE_CACHE", {})
    assert ticker_map.normalize_to_yahoo("EBS", "Vienna", "AT") == "EBS.VI"
    assert ticker_map.normalize_to_yahoo("SAP", "Xetra", "DE") == "SAP.DE"
    assert ticker_map.normalize_to_yahoo("AAPL", "NASDAQ", "US") == "AAPL"
    assert ticker_map.normalize_to_yahoo("ALREADY.VI", "Vienna", "AT") == "ALREADY.VI"


def test_load_overrides_from_csv(tmp_path: Path, monkeypatch):
    p = tmp_path / "ticker_overrides.csv"
    pl.DataFrame({"raw": ["FOO", "BAR"], "yahoo": ["FOO.VI", "BAR.DE"]}).write_csv(str(p))
    monkeypatch.setattr(ticker_map, "_OVERRIDE_CACHE", None)
    monkeypatch.setattr(ticker_map, "_OVERRIDE_CACHE_SOURCE", None)
    overrides = ticker_map._load_overrides(path=p)
    assert overrides["FOO"] == "FOO.VI"
    assert overrides["BAR"] == "BAR.DE"


def test_load_overrides_uses_cached_file_contents(tmp_path: Path, monkeypatch):
    p = tmp_path / "ticker_overrides.csv"
    pl.DataFrame({"raw": ["FOO"], "yahoo": ["FOO.VI"]}).write_csv(str(p))
    monkeypatch.setattr(ticker_map, "_OVERRIDE_CACHE", None)
    monkeypatch.setattr(ticker_map, "_OVERRIDE_CACHE_SOURCE", None)
    first = ticker_map._load_overrides(path=p)
    p.write_text("raw,yahoo\nFOO,FOO.DE\n", encoding="utf-8")
    second = ticker_map._load_overrides(path=p)
    assert first == second


def test_ticker_map_load_overrides_branches_and_cache_reset(tmp_path: Path, monkeypatch, capsys):
    ticker_map.clear_override_cache()
    assert ticker_map._load_overrides() == {}

    missing = tmp_path / "missing.csv"
    assert ticker_map._load_overrides(missing) == {}

    malformed = tmp_path / "bad.csv"
    malformed.write_text("raw,yahoo\n", encoding="utf-8")
    monkeypatch.setattr(ticker_map.pl, "read_csv", lambda path: (_ for _ in ()).throw(RuntimeError("bad csv")))
    assert ticker_map._load_overrides(malformed) == {}
    assert "failed to read ticker override file" in capsys.readouterr().out
    monkeypatch.setattr(ticker_map.pl, "read_csv", lambda path: (_ for _ in ()).throw(OSError("gone")))
    assert ticker_map._load_overrides(malformed) == {}

    monkeypatch.setattr(ticker_map.pl, "read_csv", lambda path: pl.DataFrame({"raw": ["AAA"]}))
    assert ticker_map._load_overrides(malformed) == {}

    ticker_map.clear_override_cache()
    assert ticker_map._OVERRIDE_CACHE is None
    assert ticker_map._OVERRIDE_CACHE_SOURCE is None


def test_ticker_map_normalize_empty_override_and_default_cases(monkeypatch):
    monkeypatch.setattr(ticker_map, "_load_overrides", lambda path=None: {"ABC": "ABC.VI"})
    assert ticker_map.normalize_to_yahoo("", "Vienna", "AT") == ""
    assert ticker_map.normalize_to_yahoo("ABC", None, None) == "ABC.VI"
    monkeypatch.setattr(ticker_map, "_load_overrides", lambda path=None: {})
    assert ticker_map.normalize_to_yahoo("XYZ", "Other", "CH") == "XYZ"
