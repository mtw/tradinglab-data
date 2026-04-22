from __future__ import annotations

import tradinglab_data.cli as cli


def test_cli_crypto_list_symbols_uses_workflow(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "crypto_list_symbols_from_config", lambda cfg, exchange=None: ["BTC_USDT", "ETH_USDT"])

    rc = cli.main(["--config", str(config_path), "crypto", "list-symbols", "--exchange", "binance"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "BTC_USDT" in out
    assert "ETH_USDT" in out


def test_cli_crypto_backfill_dispatches(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        cli,
        "crypto_backfill_from_config",
        lambda cfg, **kwargs: calls.append(kwargs)
        or {
            "exchange": "binance",
            "market_type": "spot",
            "interval": "1h",
            "universe": "crypto_majors",
            "symbols": ["BTC_USDT"],
            "files_written": 1,
            "rows_written": 2,
            "root": str(tmp_path / "crypto"),
        },
    )

    rc = cli.main(["--config", str(config_path), "crypto", "backfill", "--interval", "1h", "--symbols", "BTC_USDT"])

    assert rc == 0
    assert calls == [{"exchange": None, "interval": "1h", "universe": None, "symbols_override": ["BTC_USDT"]}]


def test_cli_crypto_validate_raises_on_invalid_result(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli,
        "crypto_validate_from_config",
        lambda cfg, **kwargs: {
            "ok": False,
            "exchange": "binance",
            "market_type": "spot",
            "interval": "1h",
            "universe": "crypto_majors",
            "root": str(tmp_path / "crypto"),
            "files_checked": 1,
            "dirty_files": ["/tmp/crypto/BTC_USDT.parquet"],
            "errors": ["broken"],
        },
    )

    try:
        cli.main(["--config", str(config_path), "crypto", "validate", "--interval", "1h", "--symbols", "BTC_USDT"])
    except SystemExit as exc:
        assert "broken" in str(exc)
    else:
        raise AssertionError("expected SystemExit")


def test_cli_crypto_refresh_universe_dispatches(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        cli,
        "crypto_refresh_universe_from_config",
        lambda cfg, **kwargs: calls.append(kwargs)
        or {
            "provider": "coingecko",
            "exchange": "binance",
            "market_type": "spot",
            "universe": "crypto_dynamic",
            "registry_path": str(tmp_path / "registry.json"),
            "universe_path": str(tmp_path / "crypto_dynamic.json"),
            "candidates_seen": 2,
            "symbols_selected": ["BTC_USDT", "ETH_USDT"],
        },
    )

    rc = cli.main(["--config", str(config_path), "crypto", "refresh-universe", "--provider", "coingecko", "--universe", "crypto_dynamic", "--limit", "2"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "CRYPTO_REFRESH_UNIVERSE" in out
    assert calls == [{"exchange": None, "provider_name": "coingecko", "universe": "crypto_dynamic", "limit": 2}]


def test_cli_crypto_show_diff_inspect_and_prune_dispatch(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "crypto_show_universe_from_config", lambda cfg, **kwargs: ["BTC_USDT", "ETH_USDT"])
    monkeypatch.setattr(
        cli,
        "crypto_diff_universe_from_config",
        lambda cfg, **kwargs: {"left_universe": "a", "right_universe": "b", "left_only": ["BTC_USDT"], "right_only": ["DOGE_USDT"], "shared": ["ETH_USDT"]},
    )
    monkeypatch.setattr(
        cli,
        "crypto_inspect_from_config",
        lambda cfg, **kwargs: [{"symbol": "BTC_USDT", "exists": True, "rows": 3, "start": "2026-04-18T00:00:00", "end": "2026-04-18T02:00:00", "path": "/tmp/BTC_USDT.parquet"}],
    )
    monkeypatch.setattr(cli, "crypto_prune_from_config", lambda cfg, **kwargs: ["/tmp/DOGE_USDT.parquet"])

    assert cli.main(["--config", str(config_path), "crypto", "show-universe", "--universe", "crypto_majors"]) == 0
    assert "BTC_USDT" in capsys.readouterr().out
    assert cli.main(["--config", str(config_path), "crypto", "diff-universe", "--left-universe", "a", "--right-universe", "b"]) == 0
    assert "left_only=BTC_USDT" in capsys.readouterr().out
    assert cli.main(["--config", str(config_path), "crypto", "inspect", "--interval", "1h", "--symbols", "BTC_USDT"]) == 0
    assert "exists=True" in capsys.readouterr().out
    assert cli.main(["--config", str(config_path), "crypto", "prune", "--interval", "1h", "--universe", "crypto_majors"]) == 0
    assert "CRYPTO_PRUNE" in capsys.readouterr().out
