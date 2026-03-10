from __future__ import annotations

from pathlib import Path

import pytest

import tradinglab_data.cli as cli


def test_cli_schema_does_not_require_config(capsys):
    rc = cli.main(["schema", "--format", "json"])
    out = capsys.readouterr().out
    assert rc == 0
    assert '"daily"' in out


def test_cli_schema_writes_output(tmp_path: Path):
    out = tmp_path / "schema.md"
    rc = cli.main(["schema", "--format", "markdown", "--out", str(out)])
    assert rc == 0
    assert out.exists()
    assert "TradingLab Data Parquet Schema" in out.read_text(encoding="utf-8")


def test_cli_update_missing_config_has_clear_error():
    with pytest.raises(FileNotFoundError, match="Create a config from"):
        cli.main(["--config", "does-not-exist.yaml", "update"])
