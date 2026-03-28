from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
import os
from pathlib import Path
from typing import Any

import yaml


PACKAGE_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = PACKAGE_ROOT / "configs"
DEFAULT_CONFIG_BASENAME = "config.yaml"
DEFAULT_CONFIG_PATH = CONFIGS_DIR / DEFAULT_CONFIG_BASENAME
DEFAULT_CONFIG_ENVVAR = "TRADINGLAB_DATA_CONFIG"
PACKAGED_CONFIG_EXAMPLE = "config.yaml.example"


def packaged_config_example_text() -> str:
    return files("tradinglab_data").joinpath(PACKAGED_CONFIG_EXAMPLE).read_text(encoding="utf-8")


def _repo_configs_available() -> bool:
    return CONFIGS_DIR.exists() and (CONFIGS_DIR / PACKAGED_CONFIG_EXAMPLE).exists()


def default_config_path() -> Path:
    env_path = os.getenv(DEFAULT_CONFIG_ENVVAR)
    if env_path:
        return Path(_expand_string(env_path))
    if _repo_configs_available() and DEFAULT_CONFIG_PATH.exists():
        return DEFAULT_CONFIG_PATH
    cwd_default = Path.cwd() / DEFAULT_CONFIG_BASENAME
    if cwd_default.exists():
        return cwd_default
    cwd_configs_default = Path.cwd() / "configs" / DEFAULT_CONFIG_BASENAME
    if cwd_configs_default.exists():
        return cwd_configs_default
    if _repo_configs_available():
        return DEFAULT_CONFIG_PATH
    return cwd_default


def _expand_string(value: str) -> str:
    return os.path.expanduser(os.path.expandvars(value))


def _expand_value(value: Any) -> Any:
    if isinstance(value, str):
        return _expand_string(value)
    if isinstance(value, list):
        return [_expand_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_value(item) for key, item in value.items()}
    return value


def resolve_config_path(path: str | Path) -> Path:
    p = Path(_expand_string(str(path)))
    candidates = [p]
    if not p.is_absolute():
        if p.parent == Path("."):
            candidates.append(Path.cwd() / "configs" / p.name)
        if _repo_configs_available():
            candidates.append(PACKAGE_ROOT / p)
            if p.parent == Path("."):
                candidates.append(CONFIGS_DIR / p.name)
    seen: set[Path] = set()
    for cand in candidates:
        resolved = cand.resolve(strict=False)
        if resolved in seen:
            continue
        seen.add(resolved)
        if cand.exists():
            return cand
    return p


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]
    source_path: Path | None = None

    @staticmethod
    def load(path: str | Path) -> "Config":
        p = resolve_config_path(path)
        if not p.exists():
            raise FileNotFoundError(
                f"Config not found: {p}. "
                "Create a config from the bundled config.yaml.example template "
                f"or pass --config /path/to/config.yaml. You can also set {DEFAULT_CONFIG_ENVVAR}."
            )
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"{p.name} must parse to a dict")
        return Config(raw=_expand_value(data), source_path=p)

    def get(self, *keys: str, default=None):
        cur: Any = self.raw
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def path(self, *keys: str, default=None) -> Path | None:
        value = self.get(*keys, default=default)
        if value is None:
            return None
        if isinstance(value, Path):
            return value
        return Path(_expand_string(str(value)))


def _require_path(cfg: Config, section: str, key: str) -> Path:
    value = cfg.path(section, key)
    if value is None:
        raise ValueError(f"Missing {section}.{key} in {cfg.source_path or 'config'}")
    return value


def universe_csv_path(cfg: Config) -> Path:
    return _require_path(cfg, "paths", "universe_csv")


def meta_root_path(cfg: Config) -> Path:
    return cfg.path("paths", "meta_root") or universe_csv_path(cfg).parent


def universe_dir_path(cfg: Config) -> Path:
    return cfg.path("paths", "universe_dir") or (meta_root_path(cfg) / "universes")


def update_log_path(cfg: Config) -> Path:
    return cfg.path("paths", "update_log_csv") or (meta_root_path(cfg) / "update_log.csv")


def ticker_overrides_path(cfg: Config) -> Path:
    return cfg.path("paths", "ticker_overrides_csv") or (meta_root_path(cfg) / "ticker_overrides.csv")


def parquet_root_path(cfg: Config) -> Path:
    return _require_path(cfg, "paths", "parquet_root")


def intraday_root_path(cfg: Config) -> Path:
    return cfg.path("extended_hours", "intraday_root") or (parquet_root_path(cfg).parent / "intraday")


def runs_root_path(cfg: Config) -> Path:
    return _require_path(cfg, "paths", "runs_root")


def registry_root_path(cfg: Config) -> Path:
    return cfg.path("paths", "registry_root") or (runs_root_path(cfg) / "runs_registry")
