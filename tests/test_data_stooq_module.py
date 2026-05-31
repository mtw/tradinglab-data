from __future__ import annotations

from urllib.error import URLError

import polars as pl
import pytest

import tradinglab_data.data_stooq as stooq


def test_stooq_symbol_from_yahoo_basic_mappings():
    assert stooq.stooq_symbol_from_yahoo("AAPL") == "aapl.us"
    assert stooq.stooq_symbol_from_yahoo("EBS.VI") == "ebs.at"
    assert stooq.stooq_symbol_from_yahoo("ADS.DE") == "ads.de"
    assert stooq.stooq_symbol_from_yahoo("") == ""
    assert stooq.stooq_symbol_from_yahoo("ABC.SW") == "abc.ch"
    assert stooq.stooq_symbol_from_yahoo("ABC.XX") == "abc.xx"


def test_infer_currency_from_symbol():
    assert stooq.infer_currency_from_symbol("AAPL") == "USD"
    assert stooq.infer_currency_from_symbol("EBS.VI") == "EUR"
    assert stooq.infer_currency_from_symbol("XYZ.UNKNOWN") == "UNKNOWN"


def test_parse_stooq_csv_text_normalizes_to_tradinglab_schema():
    text = "Date,Open,High,Low,Close,Volume\n2020-01-02,100,110,90,105,12345\n"
    df = stooq._parse_stooq_csv_text(text)
    assert df.height == 1
    assert df.columns == ["date", "open", "high", "low", "close", "volume", "adj_close"]
    row = df.to_dicts()[0]
    assert row["open"] == 100.0
    assert row["close"] == 105.0
    assert row["volume"] == 12345.0


def test_parse_stooq_csv_text_empty_returns_canonical_empty_frame():
    df = stooq._parse_stooq_csv_text("")

    assert df.is_empty()
    assert dict(df.schema) == {
        "date": pl.Datetime,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "adj_close": pl.Float64,
        "volume": pl.Float64,
    }


def test_parse_stooq_csv_text_handles_empty_parsed_frame_and_missing_required_columns(monkeypatch):
    monkeypatch.setattr(stooq.pl, "read_csv", lambda *args, **kwargs: pl.DataFrame())
    assert stooq._parse_stooq_csv_text("Date,Open\n") .is_empty()
    monkeypatch.setattr(stooq.pl, "read_csv", lambda *args, **kwargs: pl.DataFrame({"Date": ["2020-01-01"], "Open": [1.0]}))
    assert stooq._parse_stooq_csv_text("Date,Open\n2020-01-01,1\n").is_empty()


def test_parse_stooq_csv_text_missing_volume_fills_zero():
    text = "Date,Open,High,Low,Close\n2020-01-02,100,110,90,105\n"
    df = stooq._parse_stooq_csv_text(text)

    assert df.height == 1
    assert df.get_column("volume").to_list() == [0.0]
    assert df.get_column("adj_close").to_list() == [105.0]


def test_fetch_stooq_history_falls_back_to_next_candidate(monkeypatch):
    requested_urls: list[str] = []

    class _FakeResponse:
        def __init__(self, text: str):
            self._text = text

        def read(self):
            return self._text.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(url: str, timeout: int = 30):
        requested_urls.append(url)
        if "ebs.at" in url:
            raise URLError("primary failed")
        return _FakeResponse("Date,Open,High,Low,Close,Volume\n2020-01-02,100,110,90,105,12345\n")

    monkeypatch.setattr(stooq, "urlopen", fake_urlopen)

    df = stooq.fetch_stooq_history(stooq.StooqDownloadSpec(symbol="EBS.VI"))

    assert df.height == 1
    assert any("ebs.at" in url for url in requested_urls)
    assert any("ebs.vi" in url for url in requested_urls)
    assert df.get_column("close").to_list() == [105.0]


def test_fetch_stooq_history_empty_symbol_and_all_failures(monkeypatch):
    assert stooq.fetch_stooq_history(stooq.StooqDownloadSpec(symbol="")).is_empty()
    monkeypatch.setattr(stooq, "urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("nope")))
    assert stooq.fetch_stooq_history(stooq.StooqDownloadSpec(symbol="AAPL")).is_empty()


@pytest.mark.network
def test_fetch_stooq_history_live_smoke():
    df = stooq.fetch_stooq_history(stooq.StooqDownloadSpec(symbol="AAPL"))

    if df.is_empty():
        pytest.skip("live Stooq fetch returned no data")
    assert isinstance(df, pl.DataFrame)
