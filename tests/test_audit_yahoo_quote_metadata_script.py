from __future__ import annotations

import json
import sys
from pathlib import Path

from tests._load import load_script_module
from tradinglab_data.yahoo_quote_audit import YahooQuoteAuditRow

mod = load_script_module("audit_yahoo_quote_metadata")


def test_main_writes_json_and_respects_fail_on_mismatch(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir()
    (universe_dir / "etf_all.csv").write_text("symbol,exchange,currency\nMVEU.L,LSE,EUR\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: \"{universe_dir}\"",
            ]
        ),
        encoding="utf-8",
    )
    out_path = tmp_path / "out.json"

    monkeypatch.setattr(
        mod,
        "audit_universe_file",
        lambda *args, **kwargs: [
            YahooQuoteAuditRow(
                symbol="MVEU.L",
                local_exchange="LSE",
                local_currency="EUR",
                local_name="MVEU",
                yahoo_exchange_display="LSE",
                yahoo_exchange="LSE",
                yahoo_currency="EUR",
                yahoo_name="MVEU",
                status="match",
                issue="",
                requested_url="u1",
                final_url="u2",
                page_symbol="MVEU.L",
                isin="",
            )
        ],
    )
    monkeypatch.setattr(mod, "make_browser_snapshot_fetcher", lambda timeout=20.0: (lambda symbol: None, lambda: None))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit_yahoo_quote_metadata.py",
            "--config",
            str(config_path),
            "--format",
            "json",
            "--out",
            str(out_path),
            "--fail-on-mismatch",
        ],
    )

    rc = mod.main()

    assert rc == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload[0]["symbol"] == "MVEU.L"
    assert payload[0]["status"] == "match"


def test_main_returns_nonzero_when_requested(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir()
    (universe_dir / "etf_all.csv").write_text("symbol,exchange,currency\nIQQ5.DE,XETRA,EUR\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: \"{universe_dir}\"",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        mod,
        "audit_universe_file",
        lambda *args, **kwargs: [
            YahooQuoteAuditRow(
                symbol="IQQ5.DE",
                local_exchange="XETRA",
                local_currency="EUR",
                local_name="IQQ5",
                yahoo_exchange_display="LSE",
                yahoo_exchange="LSE",
                yahoo_currency="GBP",
                yahoo_name="IQQ5",
                status="exchange_currency_mismatch",
                issue="",
                requested_url="u1",
                final_url="u2",
                page_symbol="IQQ5.DE",
                isin="",
            )
        ],
    )
    monkeypatch.setattr(mod, "make_browser_snapshot_fetcher", lambda timeout=20.0: (lambda symbol: None, lambda: None))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit_yahoo_quote_metadata.py",
            "--config",
            str(config_path),
            "--fail-on-mismatch",
        ],
    )

    rc = mod.main()

    assert rc == 1


def test_main_uses_http_fetcher_when_requested(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir()
    (universe_dir / "etf_all.csv").write_text("symbol,exchange,currency\nMVEU.L,LSE,EUR\n", encoding="utf-8")
    config_path.write_text("\n".join(["paths:", f"  universe_dir: \"{universe_dir}\""]), encoding="utf-8")

    called: dict[str, object] = {}

    def fake_audit(*args, **kwargs):
        called["fetcher"] = kwargs.get("fetcher")
        return []

    monkeypatch.setattr(mod, "audit_universe_file", fake_audit)
    monkeypatch.setattr(mod, "make_browser_snapshot_fetcher", lambda timeout=20.0: (_ for _ in ()).throw(AssertionError("should not build browser fetcher")))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit_yahoo_quote_metadata.py",
            "--config",
            str(config_path),
            "--fetcher",
            "http",
        ],
    )

    rc = mod.main()

    assert rc == 0
    assert called["fetcher"] is None
