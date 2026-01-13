"""Context management for structured logging.

Provides context binding for pipeline tracking (pipeline_id, source, etc.).
"""

from contextlib import contextmanager
from typing import Any

import structlog


@contextmanager
def bind_context(**kwargs: Any) -> Any:
    """Context manager to bind variables to all logs within the context.

    All log calls within this context will automatically include the bound variables.

    Args:
        **kwargs: Key-value pairs to bind to the logging context.

    Yields:
        None

    Example:
        >>> with bind_context(pipeline_id="pipeline_123", source="coingecko"):
        ...     logger.info("starting_ingestion")
        ...     # This log will include pipeline_id and source automatically
    """
    # Bind context variables
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(**kwargs)

    try:
        yield
    finally:
        # Clear context when exiting
        structlog.contextvars.clear_contextvars()


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()


def get_context() -> dict[str, Any]:
    """Get current context variables.

    Returns:
        Dictionary of currently bound context variables.
    """
    return dict(structlog.contextvars.get_contextvars())
