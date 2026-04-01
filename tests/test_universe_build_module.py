from __future__ import annotations

from pathlib import Path

import pytest

import tradinglab_data.universe_build as ub


def test_merge_rows_adds_index_memberships():
    rows = ub._merge_rows({"sp500": [{"symbol": "AAPL", "isin": None, "source": "x"}], "djia": [{"symbol": "AAPL", "isin": None, "source": "y"}]})
    assert len(rows) == 1
    assert "DJIA" in (rows[0].get("index_memberships") or "")
    assert "SP500" in (rows[0].get("index_memberships") or "")


def test_build_universe_with_mocked_source(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(ub, "_sp500_rows", lambda: [{"symbol": "AAPL", "name": "Apple", "exchange": "NASDAQ", "country": "US", "source": "sp500_x", "active": 1, "isin": None}])
    monkeypatch.setattr(ub, "normalize_to_yahoo", lambda symbol, exchange, country, **kwargs: symbol)
    out = tmp_path / "universe.csv"
    df = ub.build_universe(indices=["sp500"], out_path=out, active_only=True)
    assert df.height == 1
    assert df.get_column("symbol").to_list() == ["AAPL"]
    assert out.exists()


def test_build_universe_falls_back_to_override_csv_when_fetch_is_empty(tmp_path: Path, monkeypatch):
    overrides_dir = tmp_path / "overrides"
    overrides_dir.mkdir(parents=True, exist_ok=True)
    (overrides_dir / "sp500.csv").write_text(
        "symbol,name,exchange,country,active,isin\nMSFT,Microsoft,NASDAQ,US,1,\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ub, "_sp500_rows", lambda: [])
    monkeypatch.setattr(ub, "normalize_to_yahoo", lambda symbol, exchange, country, **kwargs: symbol)

    out = tmp_path / "universe.csv"
    df = ub.build_universe(
        indices=["sp500"],
        out_path=out,
        active_only=True,
        overrides_dir=overrides_dir,
    )

    assert df.height == 1
    assert df.get_column("symbol").to_list() == ["MSFT"]
    assert df.get_column("source").to_list() == ["sp500_override"]


def test_build_universe_marks_isin_only_rows_for_mapping(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        ub,
        "_sp500_rows",
        lambda: [
            {
                "symbol": "",
                "name": "Mapped Corp",
                "exchange": "Xetra",
                "country": "Germany",
                "source": "sp500_x",
                "active": 1,
                "isin": "DE0001234567",
            }
        ],
    )
    normalize_calls: list[tuple[str, str | None, str | None]] = []

    def fake_normalize(symbol, exchange, country, **kwargs):
        normalize_calls.append((symbol, exchange, country))
        return "MAPPED.DE"

    monkeypatch.setattr(ub, "normalize_to_yahoo", fake_normalize)

    out = tmp_path / "universe.csv"
    df = ub.build_universe(indices=["sp500"], out_path=out, active_only=True)

    assert df.get_column("symbol").to_list() == ["MAPPED.DE"]
    assert df.get_column("needs_mapping").to_list() == [1]
    assert normalize_calls == [("DE0001234567", "Xetra", "Germany")]


def test_build_universe_raises_when_all_sources_and_overrides_are_empty(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(ub, "_sp500_rows", lambda: [])
    monkeypatch.setattr(ub, "normalize_to_yahoo", lambda symbol, exchange, country, **kwargs: symbol)

    with pytest.raises(RuntimeError, match="No constituents found"):
        ub.build_universe(
            indices=["sp500"],
            out_path=tmp_path / "universe.csv",
            active_only=True,
            overrides_dir=tmp_path / "missing-overrides",
        )
