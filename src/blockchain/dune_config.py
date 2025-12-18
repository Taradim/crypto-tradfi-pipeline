"""Configuration loader for Dune query specs used by dlt pipelines.

This module implements a small, typed layer over `.dlt/config.toml` to:
- Make configuration errors fail fast (missing keys, typos, wrong types).
- Normalize fields (e.g., `primary_key` as `tuple[str, ...] | None`).
- Keep the rest of the codebase independent from the raw TOML structure.

Schema (inspired by the Medium article):

```toml
[dune_queries.dex_volume]
query = "https://dune.com/queries/4388"
primary_key = ["project", "_col1"]
write_disposition = "merge"

[dune_queries.dex_volume_incremental]
query = "https://dune.com/queries/4778954"
primary_key = ["project", "date"]
write_disposition = "merge"
replication_key = "date"
starting_replication_value = "2025-01-01"
```
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal, Mapping, Sequence

WriteDisposition = Literal["merge", "append", "replace"]

_DEFAULT_NAMESPACE: Final[str] = "dune_queries"


class DuneConfigError(ValueError):
    """Raised when the Dune TOML config is missing required fields or is invalid."""


@dataclass(frozen=True, slots=True)
class IncrementalConfig:
    """Incremental loading configuration for a query.

    Attributes:
        replication_key: Column name used as the incremental cursor.
        starting_replication_value: Initial cursor value (string, interpreted downstream).
    """

    replication_key: str
    starting_replication_value: str


@dataclass(frozen=True, slots=True)
class DuneQuerySpec:
    """Typed representation of one query entry in `.dlt/config.toml`.

    Attributes:
        name: Resource/table name (derived from TOML section key).
        query: Dune query URL or id (we start with URL support in v1).
        primary_key: Optional primary key fields (normalized to an immutable tuple).
        write_disposition: How dlt should write into the destination table.
        incremental: Optional incremental configuration.
        parameters: Optional query parameters (reserved for later; not required for MVP).
    """

    name: str
    query: str
    primary_key: tuple[str, ...] | None
    write_disposition: WriteDisposition
    incremental: IncrementalConfig | None
    parameters: Mapping[str, Any] | None = None


def find_project_root(start: Path | None = None) -> Path:
    """Find the project root by walking up until `pyproject.toml` is found.

    Args:
        start: Starting directory. Defaults to current working directory.

    Returns:
        The closest ancestor directory that contains `pyproject.toml`. If none is
        found, returns the resolved `start` directory.
    """

    base = (start or Path.cwd()).resolve()
    for candidate in (base, *base.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return base


def default_config_path(project_root: Path | None = None) -> Path:
    """Return the default `.dlt/config.toml` path for this project."""

    root = project_root or find_project_root()
    return root / ".dlt" / "config.toml"


def load_dune_query_specs(
    *,
    config_path: Path | None = None,
    namespace: str = _DEFAULT_NAMESPACE,
    strict: bool = True,
) -> list[DuneQuerySpec]:
    """Load and validate query specs from a Dune TOML config file.

    Args:
        config_path: Path to the TOML config file. Defaults to `.dlt/config.toml`
            resolved from the project root.
        namespace: Top-level TOML namespace (defaults to `dune_queries`).
        strict: If True, unknown keys in query specs raise `DuneConfigError`.

    Returns:
        A deterministic list of `DuneQuerySpec` sorted by name.

    Raises:
        DuneConfigError: If the config is missing required keys or has invalid types.
    """

    path = config_path or default_config_path()
    if not path.is_file():
        raise DuneConfigError(
            f"Config file not found at: {path}. "
            "Create it (e.g. `.dlt/config.toml`) or pass `config_path=` explicitly."
        )

    with path.open("rb") as f:
        raw = tomllib.load(f)

    if not isinstance(raw, dict):
        raise DuneConfigError("Invalid TOML: expected a top-level table/object.")

    ns = raw.get(namespace)
    if ns is None:
        raise DuneConfigError(f"Missing top-level namespace [{namespace}.*] in {path}.")
    if not isinstance(ns, dict):
        raise DuneConfigError(
            f"Invalid TOML: `{namespace}` must be a table/object (got {type(ns).__name__})."
        )

    specs: list[DuneQuerySpec] = []
    for name in sorted(ns.keys()):
        entry = ns[name]
        if not isinstance(entry, dict):
            raise DuneConfigError(
                f"Invalid spec for `{namespace}.{name}`: expected a table/object."
            )
        specs.append(_parse_query_spec(name=name, entry=entry, strict=strict))

    return specs


def _parse_query_spec(
    *, name: str, entry: Mapping[str, Any], strict: bool
) -> DuneQuerySpec:
    allowed_keys = {
        "query",
        "primary_key",
        "write_disposition",
        "replication_key",
        "starting_replication_value",
        "parameters",
    }
    unknown = set(entry.keys()) - allowed_keys
    if strict and unknown:
        raise DuneConfigError(
            f"Unknown keys in `{_DEFAULT_NAMESPACE}.{name}`: {sorted(unknown)}. "
            "Fix typos or disable strict mode."
        )

    query = entry.get("query")
    if not isinstance(query, str) or not query.strip():
        raise DuneConfigError(
            f"Invalid `{_DEFAULT_NAMESPACE}.{name}.query`: expected a non-empty string."
        )

    write_disposition = entry.get("write_disposition")
    if write_disposition not in ("merge", "append", "replace"):
        raise DuneConfigError(
            f"Invalid `{_DEFAULT_NAMESPACE}.{name}.write_disposition`: "
            "expected one of {'merge','append','replace'}."
        )

    primary_key = _normalize_primary_key(entry.get("primary_key"))

    replication_key = entry.get("replication_key")
    starting_value = entry.get("starting_replication_value")
    incremental: IncrementalConfig | None = None
    if replication_key is None and starting_value is None:
        incremental = None
    else:
        if not isinstance(replication_key, str) or not replication_key.strip():
            raise DuneConfigError(
                f"Invalid `{_DEFAULT_NAMESPACE}.{name}.replication_key`: expected a non-empty string."
            )
        if not isinstance(starting_value, str) or not starting_value.strip():
            raise DuneConfigError(
                f"Invalid `{_DEFAULT_NAMESPACE}.{name}.starting_replication_value`: "
                "expected a non-empty string."
            )
        incremental = IncrementalConfig(
            replication_key=replication_key.strip(),
            starting_replication_value=starting_value.strip(),
        )

    parameters = entry.get("parameters")
    if parameters is not None and not isinstance(parameters, dict):
        raise DuneConfigError(
            f"Invalid `{_DEFAULT_NAMESPACE}.{name}.parameters`: expected a table/object."
        )

    return DuneQuerySpec(
        name=name,
        query=query.strip(),
        primary_key=primary_key,
        write_disposition=write_disposition,
        incremental=incremental,
        parameters=parameters,
    )


def _normalize_primary_key(value: Any) -> tuple[str, ...] | None:
    """Normalize `primary_key` from TOML (str | list[str]) into a tuple or None."""

    if value is None:
        return None
    if isinstance(value, str):
        key = value.strip()
        if not key:
            raise DuneConfigError("Invalid `primary_key`: expected a non-empty string.")
        return (key,)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        keys: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise DuneConfigError(
                    "Invalid `primary_key`: expected a string or a list of non-empty strings."
                )
            keys.append(item.strip())
        if not keys:
            raise DuneConfigError("Invalid `primary_key`: list must not be empty.")
        return tuple(keys)
    raise DuneConfigError(
        "Invalid `primary_key`: expected a string or a list of strings."
    )
