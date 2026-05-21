from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path

from tests._load import load_script_module

mod = load_script_module("check_crypto_status")


def test_check_crypto_status_lists_universes_and_exits(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "meta" / "universes"
    crypto_universe_dir = tmp_path / "meta" / "crypto" / "universes"
    universe_dir.mkdir(parents=True)
    crypto_universe_dir.mkdir(parents=True)
    (universe_dir / "sp500.csv").write_text("symbol,active\nAAA,1\n", encoding="utf-8")
    (crypto_universe_dir / "crypto_dynamic.json").write_text(
        '{\n  "universe": "crypto_dynamic",\n  "symbols": ["BTC_USDT"]\n}\n',
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
    monkeypatch.setattr(
        sys,
        "argv",
        ["check_crypto_status.py", "--config", str(config_path), "--list-universes"],
    )

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        mod.main()

    printed = out.getvalue()
    assert "sp500: 1 symbols" in printed
    assert "crypto_dynamic: 1 symbols" in printed
