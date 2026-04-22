from __future__ import annotations

import os
from pathlib import Path

import pytest

from tradinglab_data import config as config_mod
from tradinglab_data.config import (
    Config,
    crypto_metadata_root_path,
    crypto_registry_path,
    crypto_root_path,
    crypto_universe_dir_path,
    default_config_path,
    intraday_root_path,
    packaged_config_example_text,
    registry_root_path,
    resolve_config_path,
    ticker_overrides_path,
    universe_dir_path,
    update_log_path,
    update_warning_state_path,
)


def test_config_load_missing_has_clear_message():
    with pytest.raises(FileNotFoundError, match="Create a config from"):
        Config.load("does-not-exist.yaml")


def test_config_load_reads_yaml(tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("paths:\n  parquet_root: data/parquet/daily\n", encoding="utf-8")
    loaded = Config.load(cfg)
    assert loaded.get("paths", "parquet_root") == "data/parquet/daily"


def test_config_load_expands_home_env_and_nested_values(tmp_path: Path, monkeypatch):
    home_dir = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("TLAB_ROOT", str(tmp_path / "root"))
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "\n".join(
            [
                "paths:",
                "  parquet_root: $HOME/data/parquet",
                "  universe_csv: ~/data/universe.csv",
                "nested:",
                "  report_dir: $TLAB_ROOT/reports",
                "  outputs:",
                "    - ~/runs/latest",
                "    - $TLAB_ROOT/archive",
            ]
        ),
        encoding="utf-8",
    )

    loaded = Config.load(cfg)

    assert loaded.get("paths", "parquet_root") == os.path.join(str(home_dir), "data/parquet")
    assert loaded.get("paths", "universe_csv") == os.path.join(str(home_dir), "data/universe.csv")
    assert loaded.get("nested", "report_dir") == os.path.join(str(tmp_path / "root"), "reports")
    assert loaded.get("nested", "outputs") == [
        os.path.join(str(home_dir), "runs/latest"),
        os.path.join(str(tmp_path / "root"), "archive"),
    ]


def test_config_path_returns_path_for_expanded_value(tmp_path: Path, monkeypatch):
    home_dir = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home_dir))
    cfg = tmp_path / "config.yaml"
    cfg.write_text("paths:\n  universe_csv: ~/data/universe.csv\n", encoding="utf-8")

    loaded = Config.load(cfg)

    assert loaded.path("paths", "universe_csv") == home_dir / "data" / "universe.csv"
    assert loaded.path("paths", "missing") is None
    assert loaded.path("paths", "missing", default="~/fallback.csv") == home_dir / "fallback.csv"


def test_config_load_expands_strings_inside_list_of_dicts_without_touching_scalars(tmp_path: Path, monkeypatch):
    home_dir = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("CACHE_ROOT", str(tmp_path / "cache"))
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "\n".join(
            [
                "jobs:",
                "  - name: daily",
                "    out: ~/runs/daily",
                "    enabled: true",
                "    retries: 3",
                "  - name: cache",
                "    out: $CACHE_ROOT/files",
                "    enabled: false",
                "    retries: 0",
            ]
        ),
        encoding="utf-8",
    )

    loaded = Config.load(cfg)
    jobs = loaded.get("jobs")

    assert jobs == [
        {
            "name": "daily",
            "out": str(home_dir / "runs" / "daily"),
            "enabled": True,
            "retries": 3,
        },
        {
            "name": "cache",
            "out": str(tmp_path / "cache" / "files"),
            "enabled": False,
            "retries": 0,
        },
    ]


def test_config_derived_paths(tmp_path: Path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "\n".join([
            "paths:",
            f"  universe_csv: {tmp_path / 'meta' / 'universe_master.csv'}",
            f"  parquet_root: {tmp_path / 'parquet' / 'daily'}",
            f"  runs_root: {tmp_path / 'runs'}",
        ]),
        encoding="utf-8",
    )
    cfg = Config.load(cfg_file)
    assert universe_dir_path(cfg) == tmp_path / "meta" / "universes"
    assert update_log_path(cfg) == tmp_path / "meta" / "update_log.csv"
    assert update_warning_state_path(cfg) == tmp_path / "meta" / "update_warning_state.json"
    assert ticker_overrides_path(cfg) == tmp_path / "meta" / "ticker_overrides.csv"
    assert intraday_root_path(cfg) == tmp_path / "parquet" / "intraday"
    assert crypto_root_path(cfg) == tmp_path / "parquet" / "crypto"
    assert crypto_metadata_root_path(cfg) == tmp_path / "meta" / "crypto"
    assert crypto_registry_path(cfg) == tmp_path / "meta" / "crypto" / "registry.json"
    assert crypto_universe_dir_path(cfg) == tmp_path / "meta" / "crypto" / "universes"
    assert registry_root_path(cfg) == tmp_path / "runs" / "runs_registry"


def test_default_config_path_prefers_envvar(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "custom.yaml"
    monkeypatch.setenv(config_mod.DEFAULT_CONFIG_ENVVAR, str(cfg))
    assert default_config_path() == cfg


def test_default_config_path_falls_back_to_cwd_config(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("paths:\n  parquet_root: data/parquet\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(config_mod.DEFAULT_CONFIG_ENVVAR, raising=False)
    monkeypatch.setattr(config_mod, "_repo_default_config_path", lambda: None)
    assert default_config_path() == cfg


def test_default_config_path_falls_back_to_repo_config_when_env_and_cwd_missing(tmp_path: Path, monkeypatch):
    repo_cfg = tmp_path / "configs" / "config.yaml"
    repo_cfg.parent.mkdir(parents=True, exist_ok=True)
    repo_cfg.write_text("paths:\n  parquet_root: repo/parquet\n", encoding="utf-8")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.delenv(config_mod.DEFAULT_CONFIG_ENVVAR, raising=False)
    monkeypatch.setattr(config_mod, "_repo_default_config_path", lambda: repo_cfg)
    assert default_config_path() == repo_cfg


def test_resolve_config_path_checks_cwd_configs_dir(tmp_path: Path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    cfg = cfg_dir / "config.yaml"
    cfg.write_text("paths:\n  parquet_root: data/parquet\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "_discover_repo_root", lambda: None)
    monkeypatch.setattr(config_mod, "_repo_configs_dir", lambda: None)
    assert resolve_config_path("config.yaml") == cfg


def test_resolve_config_path_checks_repo_candidate_when_present(tmp_path: Path, monkeypatch):
    repo_cfg = tmp_path / "configs" / "config.yaml"
    repo_cfg.parent.mkdir(parents=True, exist_ok=True)
    repo_cfg.write_text("paths:\n  parquet_root: repo/parquet\n", encoding="utf-8")
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.setattr(config_mod, "_discover_repo_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "_repo_configs_dir", lambda: tmp_path / "configs")
    assert resolve_config_path("config.yaml") == repo_cfg


def test_config_legacy_aliases_are_resolved_lazily(monkeypatch, tmp_path: Path):
    repo_root = tmp_path / "repo"
    configs_dir = repo_root / "configs"
    default_cfg = configs_dir / "config.yaml"
    monkeypatch.setattr(config_mod, "_discover_repo_root", lambda: repo_root)
    monkeypatch.setattr(config_mod, "_repo_configs_dir", lambda: configs_dir)
    monkeypatch.setattr(config_mod, "_repo_default_config_path", lambda: default_cfg)

    assert getattr(config_mod, "PACKAGE_ROOT") == repo_root
    assert getattr(config_mod, "CONFIGS_DIR") == configs_dir
    assert getattr(config_mod, "DEFAULT_CONFIG_PATH") == default_cfg


def test_packaged_config_example_text_contains_paths():
    text = packaged_config_example_text()
    assert "paths:" in text
    assert "parquet_root:" in text
