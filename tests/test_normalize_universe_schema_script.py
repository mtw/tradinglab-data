from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from tests._load import load_script_module

mod = load_script_module("normalize_universe_schema")


def test_normalize_row_populates_canonical_fields():
    row = mod._normalize_row(
        {"symbol": "spy", "name": "SPDR S&P 500 ETF", "source": "", "country": "us", "exchange": "nysearca"},
        "etf_us",
        "2026-05-02",
    )

    assert row["symbol"] == "SPY"
    assert row["instrument_type"] == "etf"
    assert row["region"] == "US"
    assert row["asset_class"] == "equity"
    assert row["source"] == "etf_us_override"


def test_main_normalizes_universe_dir_and_rebuilds_master(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir(parents=True)
    master_out = tmp_path / "merged.csv"
    (universe_dir / "sp500.csv").write_text(
        "symbol,name,country,exchange,active\nspy,SPDR S&P 500 ETF,US,NYSEARCA,1\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {master_out}",
                f"  universe_dir: {universe_dir}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv", ["normalize_universe_schema.py", "--config", str(config_path)])

    mod.main()

    normalized = pl.read_csv(universe_dir / "sp500.csv")
    master = pl.read_csv(master_out)
    assert "instrument_type" in normalized.columns
    assert normalized.get_column("symbol").to_list() == ["SPY"]
    assert master.get_column("symbol").to_list() == ["SPY"]


def test_build_master_prefers_nonempty_later_metadata_and_unions_memberships():
    rows = [
        {
            "symbol": "SPY",
            "name": "SPDR S&P 500 ETF",
            "exchange": "",
            "country": "",
            "currency": "",
            "source": "first",
            "index_memberships": "SP500",
            "instrument_type": "etf",
        },
        {
            "symbol": "SPY",
            "name": "SPDR S&P 500 ETF Trust",
            "exchange": "NYSEARCA",
            "country": "US",
            "currency": "USD",
            "source": "second",
            "index_memberships": "ETF_ALL",
            "instrument_type": "etf",
        },
    ]

    out = mod._build_master(rows)

    assert len(out) == 1
    row = out[0]
    assert row["symbol"] == "SPY"
    assert row["name"] == "SPDR S&P 500 ETF Trust"
    assert row["exchange"] == "NYSEARCA"
    assert row["country"] == "US"
    assert row["currency"] == "USD"
    assert row["source"] == "second"
    assert row["index_memberships"] == "ETF_ALL,SP500"
