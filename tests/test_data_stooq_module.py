from __future__ import annotations

import tradinglab_data.data_stooq as stooq


def test_stooq_symbol_from_yahoo_basic_mappings():
    assert stooq.stooq_symbol_from_yahoo("AAPL") == "aapl.us"
    assert stooq.stooq_symbol_from_yahoo("EBS.VI") == "ebs.at"
    assert stooq.stooq_symbol_from_yahoo("ADS.DE") == "ads.de"


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
    assert row["volume"] == 12345
