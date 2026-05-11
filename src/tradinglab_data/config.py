from __future__ import annotations

import os
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any, Protocol

import yaml


class ConfigLike(Protocol):
    @property
    def source_path(self) -> Path | None: ...

    def get(self, *keys: str, default: Any = None) -> Any: ...

    def path(self, *keys: str, default: Any = None) -> Path | None: ...


DEFAULT_CONFIG_BASENAME = "config.yaml"
DEFAULT_CONFIG_ENVVAR = "TRADINGLAB_DATA_CONFIG"
PACKAGED_CONFIG_EXAMPLE = "config.yaml.example"


def _discover_repo_root() -> Path | None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "configs" / PACKAGED_CONFIG_EXAMPLE).exists():
            return parent
    return None


def _repo_configs_dir() -> Path | None:
    root = _discover_repo_root()
    if root is None:
        return None
    return root / "configs"


def _repo_default_config_path() -> Path | None:
    configs_dir = _repo_configs_dir()
    if configs_dir is None:
        return None
    return configs_dir / DEFAULT_CONFIG_BASENAME


def __getattr__(name: str) -> Any:
    if name == "PACKAGE_ROOT":
        return _discover_repo_root()
    if name == "CONFIGS_DIR":
        return _repo_configs_dir()
    if name == "DEFAULT_CONFIG_PATH":
        return _repo_default_config_path()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def packaged_config_example_text() -> str:
    return files("tradinglab_data").joinpath(PACKAGED_CONFIG_EXAMPLE).read_text(encoding="utf-8")


def _repo_configs_available() -> bool:
    configs_dir = _repo_configs_dir()
    return configs_dir is not None and configs_dir.exists() and (configs_dir / PACKAGED_CONFIG_EXAMPLE).exists()


def default_config_path() -> Path:
    env_path = os.getenv(DEFAULT_CONFIG_ENVVAR)
    if env_path:
        return Path(_expand_string(env_path))
    repo_default = _repo_default_config_path()
    if repo_default is not None and repo_default.exists():
        return repo_default
    cwd_default = Path.cwd() / DEFAULT_CONFIG_BASENAME
    if cwd_default.exists():
        return cwd_default
    cwd_configs_default = Path.cwd() / "configs" / DEFAULT_CONFIG_BASENAME
    if cwd_configs_default.exists():
        return cwd_configs_default
    if repo_default is not None:
        return repo_default
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
        repo_root = _discover_repo_root()
        repo_configs_dir = _repo_configs_dir()
        if repo_root is not None:
            candidates.append(repo_root / p)
        if repo_configs_dir is not None and p.parent == Path("."):
            candidates.append(repo_configs_dir / p.name)
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
    def load(path: str | Path) -> Config:
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

    def get(self, *keys: str, default: Any = None) -> Any:
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


def _require_path(cfg: ConfigLike, section: str, key: str) -> Path:
    value = cfg.path(section, key)
    if value is None:
        raise ValueError(f"Missing {section}.{key} in {cfg.source_path or 'config'}")
    return value


def universe_csv_path(cfg: ConfigLike) -> Path:
    return _require_path(cfg, "paths", "universe_csv")


def meta_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "meta_root") or universe_csv_path(cfg).parent


def universe_dir_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "universe_dir") or (meta_root_path(cfg) / "universes")


def update_log_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "update_log_csv") or (meta_root_path(cfg) / "update_log.csv")


def update_warning_state_path(cfg: ConfigLike) -> Path:
    explicit = cfg.path("paths", "update_warning_state_json")
    if explicit is not None:
        return explicit
    update_log = cfg.path("paths", "update_log_csv")
    if update_log is not None:
        return update_log.parent / "update_warning_state.json"
    return meta_root_path(cfg) / "update_warning_state.json"


def ticker_overrides_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "ticker_overrides_csv") or (meta_root_path(cfg) / "ticker_overrides.csv")


def parquet_root_path(cfg: ConfigLike) -> Path:
    return _require_path(cfg, "paths", "parquet_root")


def intraday_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("extended_hours", "intraday_root") or (parquet_root_path(cfg).parent / "intraday")


def intraday_research_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("intraday", "research_root") or (parquet_root_path(cfg).parent / "intraday_research")


def intraday_live_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("intraday_live", "live_root") or (parquet_root_path(cfg).parent / "intraday_live")


def crypto_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "crypto_root") or (parquet_root_path(cfg).parent / "crypto")


def crypto_metadata_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "crypto_metadata_root") or (meta_root_path(cfg) / "crypto")


def crypto_registry_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "crypto_registry_json") or (crypto_metadata_root_path(cfg) / "registry.json")


def crypto_universe_dir_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "crypto_universe_dir") or (crypto_metadata_root_path(cfg) / "universes")


def runs_root_path(cfg: ConfigLike) -> Path:
    return _require_path(cfg, "paths", "runs_root")


def registry_root_path(cfg: ConfigLike) -> Path:
    return cfg.path("paths", "registry_root") or (runs_root_path(cfg) / "runs_registry")
