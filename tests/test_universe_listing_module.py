from __future__ import annotations

from pathlib import Path

import pytest

import tradinglab_data.universe_listing as listing
from tradinglab_data.config import Config
from tradinglab_data.universe_listing import list_available_universes, render_available_universes


def test_list_available_universes_includes_equity_and_crypto(tmp_path: Path):
    universe_dir = tmp_path / "meta" / "universes"
    crypto_universe_dir = tmp_path / "meta" / "crypto" / "universes"
    universe_dir.mkdir(parents=True)
    crypto_universe_dir.mkdir(parents=True)
    (universe_dir / "intraday_live_core.csv").write_text("symbol,active\nAAA,1\nBBB,1\n", encoding="utf-8")
    (universe_dir / "sp500.csv").write_text("symbol,active\nAAPL,1\n", encoding="utf-8")
    (crypto_universe_dir / "crypto_dynamic.json").write_text(
        '{\n  "universe": "crypto_dynamic",\n  "symbols": ["BTC_USDT", "ETH_USDT"]\n}\n',
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {universe_dir}",
                f"  crypto_universe_dir: {crypto_universe_dir}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = Config.load(config_path)
    entries = list_available_universes(cfg)

    names = {(entry.family, entry.name): entry.symbol_count for entry in entries}
    assert names[("equity", "intraday_live_core")] == 2
    assert names[("equity", "sp500")] == 1
    assert names[("crypto", "crypto_dynamic")] == 2
    assert ("crypto", "crypto_core") in names

    rendered = render_available_universes(entries)
    assert "Equity universes:" in rendered
    assert "intraday_live_core: 2 symbols" in rendered
    assert "Crypto universes:" in rendered
    assert "crypto_dynamic: 2 symbols" in rendered


def test_render_available_universes_handles_empty_list():
    assert render_available_universes([]) == "No universes found.\n"


def test_listing_helpers_handle_missing_roots_and_broken_equity_files(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {tmp_path / 'missing-universes'}",
                f"  crypto_universe_dir: {tmp_path / 'missing-crypto'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cfg = Config.load(config_path)

    assert listing._list_equity_universes(cfg) == []

    universe_dir = tmp_path / "equity"
    universe_dir.mkdir()
    (universe_dir / "broken.csv").write_text("symbol\nAAA\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {universe_dir}",
                f"  crypto_universe_dir: {tmp_path / 'crypto'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cfg = Config.load(config_path)
    monkeypatch.setattr(listing, "load_universe_frame", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad csv")))

    entries = listing._list_equity_universes(cfg)
    assert len(entries) == 1
    assert entries[0].symbol_count == 0


def test_list_crypto_universes_marks_dynamic_without_json_file(tmp_path: Path):
    crypto_universe_dir = tmp_path / "meta" / "crypto" / "universes"
    crypto_universe_dir.mkdir(parents=True)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  crypto_universe_dir: {crypto_universe_dir}\n", encoding="utf-8")
    cfg = Config.load(config_path)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(listing, "load_crypto_universes", lambda cfg: {"crypto_dynamic_only": ("BTC_USDT",)})
    try:
        entries = listing._list_crypto_universes(cfg)
    finally:
        monkeypatch.undo()

    assert entries[0].source == "dynamic"
    assert entries[0].path == "dynamic"
