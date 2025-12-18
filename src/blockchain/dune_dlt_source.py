"""dlt source factory for Dune query specs.

This module bridges:
- `.dlt/config.toml` (data-driven query specs)
- Dune API execution (via `DuneApiClient`)
- dlt resources/sources (DuckDB destination, incremental loading, etc.)

We intentionally keep this adapter thin so that upgrading from the direct Dune API
client to Spice later is localized to a single place.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

import dlt

from .dune_client import DuneApiClient
from .dune_config import DuneQuerySpec, load_dune_query_specs

logger = logging.getLogger(__name__)


def _merge_parameters(
    *, base: Mapping[str, Any] | None, overlay: Mapping[str, Any] | None
) -> dict[str, Any] | None:
    if not base and not overlay:
        return None
    merged: dict[str, Any] = {}
    if base:
        merged.update(dict(base))
    if overlay:
        merged.update(dict(overlay))
    return merged


def _is_custom_sql(query: str) -> bool:
    """Check if query is custom SQL (not a Dune URL or query ID)."""
    q = str(query).strip()
    if not q:
        return False
    # If it's a URL or numeric ID, it's not custom SQL
    if q.isdigit() or q.startswith("http://") or q.startswith("https://"):
        return False
    # If it contains SQL keywords, assume it's custom SQL
    sql_keywords = ("SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY", "GROUP BY")
    return any(keyword in q.upper() for keyword in sql_keywords)


def _replace_sql_placeholders(
    sql: str, *, replication_key: str, cursor_value: str
) -> str:
    """Replace {replication_key} and {cursor_value} placeholders in custom SQL.

    This matches the pattern from the Medium article for incremental custom SQL queries.
    """
    return sql.replace("{replication_key}", replication_key).replace(
        "{cursor_value}", cursor_value
    )


def _spec_primary_key_for_dlt(spec: DuneQuerySpec) -> list[str] | str | None:
    if spec.primary_key is None:
        return None
    if len(spec.primary_key) == 1:
        return spec.primary_key[0]
    return list(spec.primary_key)


def _make_resource(spec: DuneQuerySpec) -> Any:
    """Create one `dlt.resource` from a single query spec."""

    primary_key = _spec_primary_key_for_dlt(spec)
    # Build resource decorator kwargs (primary_key is optional in dlt)
    resource_kwargs: dict[str, Any] = {
        "name": spec.name,
        "write_disposition": spec.write_disposition,
    }
    if primary_key is not None:
        resource_kwargs["primary_key"] = primary_key

    if spec.incremental is None:

        @dlt.resource(**resource_kwargs)
        def non_incremental_resource(
            api_key: str = dlt.secrets.value,
        ) -> Iterable[Mapping[str, Any]]:
            client = DuneApiClient(api_key=api_key)
            rows = client.run_query_rows(query=spec.query, parameters=spec.parameters)
            for row in rows:
                yield row

        return non_incremental_resource

    inc = spec.incremental

    @dlt.resource(**resource_kwargs)
    def incremental_resource(
        api_key: str = dlt.secrets.value,
        cursor: Any = dlt.sources.incremental(
            cursor_path=inc.replication_key,
            initial_value=inc.starting_replication_value,
        ),
    ) -> Iterable[Mapping[str, Any]]:
        # Handle custom SQL with placeholders (article pattern: {replication_key} and {cursor_value})
        query = spec.query
        if _is_custom_sql(query):
            query = _replace_sql_placeholders(
                query,
                replication_key=inc.replication_key,
                cursor_value=str(cursor.last_value),
            )
            # For custom SQL, parameters are embedded in the SQL string itself
            params = spec.parameters
        else:
            # For Dune query URLs/IDs, use query parameters
            incremental_param = {inc.replication_key: cursor.last_value}
            params = _merge_parameters(base=spec.parameters, overlay=incremental_param)

        client = DuneApiClient(api_key=api_key)
        rows = client.run_query_rows(query=query, parameters=params)
        for row in rows:
            yield row

    return incremental_resource


@dlt.source(name="dune_source")
def dune_source(
    *,
    config_path: Any | None = None,
    namespace: str = "dune_queries",
) -> list[Any]:
    """Create a dlt source from `.dlt/config.toml`.

    Args:
        config_path: Optional TOML config path. Defaults to `.dlt/config.toml`.
        namespace: TOML namespace to read (defaults to `dune_queries`).

    Returns:
        A list of dlt resources (one per query spec).
    """

    specs = load_dune_query_specs(config_path=config_path, namespace=namespace)
    if not specs:
        logger.warning("No query specs found under namespace=%s", namespace)
        return []
    return [_make_resource(spec) for spec in specs]
