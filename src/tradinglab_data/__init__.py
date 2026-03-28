from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "cli",
    "config",
    "contracts",
    "data_stooq",
    "data_yf",
    "extended_hours_monitor",
    "parquet_verify",
    "schema",
    "ticker_map",
    "universe",
    "universe_build",
    "workflows",
    "CoverageEntry",
    "DailyCloseInfo",
    "ExtendedHoursResult",
    "MonitorExtendedHoursResult",
    "UpdateResult",
    "VerifyResult",
    "UniverseRow",
    "build_universe",
    "canonicalize_symbol",
    "clear_currency_cache",
    "clear_override_cache",
    "load_universe",
    "load_universe_frame",
    "normalize_to_yahoo",
    "render_schema_json",
    "render_schema_markdown",
    "run_parquet_sanity_checks",
    "schema_manifest",
    "update_from_config",
    "monitor_extended_hours_from_config",
    "validate_alerts_frame",
    "validate_daily_frame",
    "validate_intraday_frame",
    "validate_moves_frame",
]

_LAZY_EXPORTS: dict[str, tuple[str, str | None]] = {
    "cli": ("tradinglab_data.cli", None),
    "config": ("tradinglab_data.config", None),
    "contracts": ("tradinglab_data.contracts", None),
    "data_stooq": ("tradinglab_data.data_stooq", None),
    "data_yf": ("tradinglab_data.data_yf", None),
    "extended_hours_monitor": ("tradinglab_data.extended_hours_monitor", None),
    "parquet_verify": ("tradinglab_data.parquet_verify", None),
    "schema": ("tradinglab_data.schema", None),
    "ticker_map": ("tradinglab_data.ticker_map", None),
    "universe": ("tradinglab_data.universe", None),
    "universe_build": ("tradinglab_data.universe_build", None),
    "workflows": ("tradinglab_data.workflows", None),
    "CoverageEntry": ("tradinglab_data.contracts", "CoverageEntry"),
    "DailyCloseInfo": ("tradinglab_data.contracts", "DailyCloseInfo"),
    "ExtendedHoursResult": ("tradinglab_data.contracts", "ExtendedHoursResult"),
    "MonitorExtendedHoursResult": ("tradinglab_data.contracts", "MonitorExtendedHoursResult"),
    "UpdateResult": ("tradinglab_data.contracts", "UpdateResult"),
    "VerifyResult": ("tradinglab_data.contracts", "VerifyResult"),
    "UniverseRow": ("tradinglab_data.universe_build", "UniverseRow"),
    "build_universe": ("tradinglab_data.universe_build", "build_universe"),
    "canonicalize_symbol": ("tradinglab_data.universe", "canonicalize_symbol"),
    "clear_currency_cache": ("tradinglab_data.data_yf", "clear_currency_cache"),
    "clear_override_cache": ("tradinglab_data.ticker_map", "clear_override_cache"),
    "load_universe": ("tradinglab_data.universe", "load_universe"),
    "load_universe_frame": ("tradinglab_data.universe", "load_universe_frame"),
    "normalize_to_yahoo": ("tradinglab_data.ticker_map", "normalize_to_yahoo"),
    "render_schema_json": ("tradinglab_data.schema", "render_schema_json"),
    "render_schema_markdown": ("tradinglab_data.schema", "render_schema_markdown"),
    "run_parquet_sanity_checks": ("tradinglab_data.parquet_verify", "run_parquet_sanity_checks"),
    "schema_manifest": ("tradinglab_data.schema", "schema_manifest"),
    "update_from_config": ("tradinglab_data.workflows", "update_from_config"),
    "monitor_extended_hours_from_config": ("tradinglab_data.workflows", "monitor_extended_hours_from_config"),
    "validate_alerts_frame": ("tradinglab_data.schema", "validate_alerts_frame"),
    "validate_daily_frame": ("tradinglab_data.schema", "validate_daily_frame"),
    "validate_intraday_frame": ("tradinglab_data.schema", "validate_intraday_frame"),
    "validate_moves_frame": ("tradinglab_data.schema", "validate_moves_frame"),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attr_name = target
    module = import_module(module_name)
    if attr_name is None:
        return module
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__) | set(_LAZY_EXPORTS))
