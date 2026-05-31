from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl

import tradinglab_data.universe_build as ub


def test_first_matching_column_returns_first_present_candidate():
    assert ub._first_matching_column({"symbol": "Symbol"}, ["ticker", "symbol"]) == "Symbol"
    assert ub._first_matching_column({"name": "Name"}, ["ticker", "symbol"]) is None


def test_safe_read_html_handles_import_value_and_generic_errors(monkeypatch, capsys):
    class FakePandas:
        def read_html(self, *args, **kwargs):
            raise ValueError("no tables")

    monkeypatch.setitem(__import__("sys").modules, "pandas", FakePandas())
    assert ub._safe_read_html("https://example.invalid") is None

    class NoisyPandas:
        def read_html(self, *args, **kwargs):
            raise RuntimeError("network down")

    monkeypatch.setitem(__import__("sys").modules, "pandas", NoisyPandas())
    assert ub._safe_read_html("https://example.invalid") is None
    assert "failed to read HTML" in capsys.readouterr().out


def test_safe_read_html_forwards_match_argument(monkeypatch):
    called: list[tuple[str, str | None]] = []

    class FakePandas:
        def read_html(self, url, match=None):
            called.append((url, match))
            return ["ok"]

    monkeypatch.setitem(__import__("sys").modules, "pandas", FakePandas())
    assert ub._safe_read_html("https://example.invalid", match="Ticker") == ["ok"]
    assert called == [("https://example.invalid", "Ticker")]


def test_wikipedia_index_rows_extracts_symbols_and_skips_blank(monkeypatch):
    monkeypatch.setattr(
        ub,
        "_safe_read_html",
        lambda *args, **kwargs: [
            pd.DataFrame({"Ticker": ["AAA", ""], "Company": ["Acme", ""]}),
        ],
    )

    rows = ub._wikipedia_index_rows(
        url="https://example.invalid",
        match="Ticker",
        symbol_columns=("ticker",),
        name_columns=("company",),
        exchange="Xetra",
        country="Germany",
        source="dax_wikipedia",
    )

    assert rows == [
        {
            "symbol": "AAA",
            "name": "Acme",
            "exchange": "Xetra",
            "country": "Germany",
            "source": "dax_wikipedia",
            "active": 1,
            "isin": None,
        }
    ]


def test_wikipedia_and_index_fetchers_return_empty_when_tables_missing(monkeypatch):
    monkeypatch.setattr(ub, "_safe_read_html", lambda *args, **kwargs: None)
    assert ub._wikipedia_index_rows(
        url="https://example.invalid",
        match="Ticker",
        symbol_columns=("ticker",),
        name_columns=("company",),
        exchange="Xetra",
        country="Germany",
        source="dax_wikipedia",
    ) == []
    assert ub._sp500_rows() == []
    assert ub._djia_rows() == []
    assert ub._atx_rows() == []
    assert ub._atx_rows_wikipedia() == []


def test_sp500_and_djia_rows_parse_expected_columns(monkeypatch):
    monkeypatch.setattr(
        ub,
        "_safe_read_html",
        lambda *args, **kwargs: [
            pd.DataFrame({"Symbol": ["AAPL"], "Security": ["Apple"], "Primary exchange": ["NASDAQ"]}),
        ],
    )
    assert ub._sp500_rows()[0]["exchange"] == "NASDAQ"

    monkeypatch.setattr(
        ub,
        "_safe_read_html",
        lambda *args, **kwargs: [
            pd.DataFrame({"Symbol": ["MSFT"], "Company": ["Microsoft"], "Exchange": ["NASDAQ"]}),
        ],
    )
    assert ub._djia_rows()[0]["name"] == "Microsoft"


def test_from_override_reads_optional_columns_and_defaults(tmp_path: Path):
    overrides = tmp_path / "overrides"
    overrides.mkdir()
    (overrides / "sp500.csv").write_text(
        "symbol,name,exchange,country,isin\n AAA ,Acme,NASDAQ,US,US123\n",
        encoding="utf-8",
    )

    rows = ub._from_override("sp500", overrides)

    assert rows[0]["symbol"] == "AAA"
    assert rows[0]["source"] == "sp500_override"
    assert rows[0]["active"] == 1


def test_stoxx_closecomposition_rows_accepts_semicolon_csv(monkeypatch):
    csv_bytes = b"Trading Symbol;Instrument;ISIN\nSIE;Siemens;DE0007236101\n"

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return csv_bytes

    monkeypatch.setattr(ub, "urlopen", lambda *args, **kwargs: FakeResponse())

    rows = ub._stoxx_closecomposition_rows("dax", "dax_stoxx")

    assert rows[0]["symbol"] == "SIE"
    assert rows[0]["isin"] == "DE0007236101"


def test_stoxx_closecomposition_read_errors_return_none(monkeypatch):
    monkeypatch.setattr(ub, "urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("blocked")))

    assert ub._read_stoxx_closecomposition("dax") is None


def test_stoxx_closecomposition_generic_error_warns(monkeypatch, capsys):
    monkeypatch.setattr(ub, "urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert ub._read_stoxx_closecomposition("dax") is None
    assert "failed to read STOXX composition" in capsys.readouterr().out


def test_atx_rows_falls_back_when_table_has_no_symbol_or_name(monkeypatch):
    monkeypatch.setattr(ub, "_safe_read_html", lambda *args, **kwargs: [pd.DataFrame({"Other": ["x"]})])
    monkeypatch.setattr(ub, "_atx_rows_wikipedia", lambda: [{"symbol": "", "name": "Fallback AG"}])

    assert ub._atx_rows() == [{"symbol": "", "name": "Fallback AG"}]


def test_atx_rows_returns_parsed_rows_when_table_has_data(monkeypatch):
    monkeypatch.setattr(ub, "_safe_read_html", lambda *args, **kwargs: [pd.DataFrame({"Symbol": ["EBS"], "Name": ["Erste"], "ISIN": ["AT0000652011"]})])
    rows = ub._atx_rows()
    assert rows[0]["symbol"] == "EBS"
    assert rows[0]["isin"] == "AT0000652011"


def test_atx_wikipedia_rows_require_name_column(monkeypatch):
    monkeypatch.setattr(ub, "_safe_read_html", lambda *args, **kwargs: [pd.DataFrame({"Name": [" Erste ", ""]})])
    assert ub._atx_rows_wikipedia()[0]["name"] == "Erste"

    monkeypatch.setattr(ub, "_safe_read_html", lambda *args, **kwargs: [pd.DataFrame({"Company": ["Erste"]})])
    assert ub._atx_rows_wikipedia() == []


def test_build_universe_ignores_unknown_index(tmp_path: Path, monkeypatch):
    monkeypatch.setitem(
        ub._INDEX_FETCHERS,
        "sp500",
        lambda: [
            {"symbol": "AAA", "name": "Active", "exchange": "NYSE", "country": "US", "source": "x", "active": 1, "isin": None},
        ],
    )
    monkeypatch.setattr(ub, "normalize_to_yahoo", lambda symbol, exchange, country, **kwargs: symbol)

    df = ub.build_universe(["unknown", "sp500"], tmp_path / "universe.csv", active_only=True)

    assert df.get_column("symbol").to_list() == ["AAA"]


def test_read_stoxx_empty_response_returns_none(monkeypatch):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b""

    monkeypatch.setattr(ub, "urlopen", lambda *args, **kwargs: FakeResponse())

    assert ub._read_stoxx_closecomposition("dax") is None


def test_stoxx_rows_empty_frame_returns_empty(monkeypatch):
    monkeypatch.setattr(ub, "_read_stoxx_closecomposition", lambda index_symbol: pl.DataFrame())

    assert ub._stoxx_closecomposition_rows("dax", "dax_stoxx") == []


def test_dax_and_mdax_row_helpers_delegate_to_wikipedia(monkeypatch):
    monkeypatch.setattr(ub, "_wikipedia_index_rows", lambda **kwargs: [{"symbol": "AAA", "source": kwargs["source"]}])
    assert ub._dax_rows_wikipedia() == [{"symbol": "AAA", "source": "dax_wikipedia"}]
    assert ub._mdax_rows_wikipedia() == [{"symbol": "AAA", "source": "mdax_wikipedia"}]
