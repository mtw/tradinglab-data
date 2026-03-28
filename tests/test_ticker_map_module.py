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
