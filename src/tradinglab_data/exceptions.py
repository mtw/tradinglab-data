from __future__ import annotations


class DataNotFoundError(Exception):
    """Raised when no data can be loaded for the given parameters."""


class UniverseNotFoundError(ValueError):
    """Raised when a universe identifier is not recognised."""
