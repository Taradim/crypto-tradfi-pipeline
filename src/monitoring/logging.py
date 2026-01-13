"""Structured logging configuration using structlog.

Provides JSON logging to file and colored console output for development.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import structlog
from structlog.types import Processor


def setup_logging(
    log_level: str | None = None,
    log_file: str | Path | None = None,
) -> None:
    """Configure structlog with JSON file output and colored console.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                   Defaults to LOG_LEVEL env var or INFO.
        log_file: Path to log file. Defaults to logs/pipeline.log.
    """
    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if log_file is None:
        log_file = Path("logs") / "pipeline.log"
    else:
        log_file = Path(log_file)

    # Create logs directory if it doesn't exist
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Convert string level to logging constant
    level = getattr(logging, log_level, logging.INFO)

    # Shared processors for both console and file
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        # format_exc_info removed - exceptions are handled automatically by structlog
    ]

    # Console renderer (colored, human-readable)
    console_processors: list[Processor] = [
        *shared_processors,
        structlog.dev.ConsoleRenderer(colors=True),
    ]

    # File renderer (JSON) - processors are used in file_formatter below

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    # Configure structlog
    structlog.configure(
        processors=console_processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Add file handler for JSON logs
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)

    # Create a separate formatter for file (JSON)
    file_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )
    file_handler.setFormatter(file_formatter)

    # Get root logger and add file handler
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)

    # Log initialization
    logger = structlog.get_logger()
    logger.info(
        "logging_initialized",
        log_level=log_level,
        log_file=str(log_file),
        console_output="colored",
        file_output="json",
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a configured logger instance.

    Args:
        name: Logger name (usually module name). If None, uses calling module.

    Returns:
        Configured structlog logger instance.
    """
    if name is None:
        # Get calling module name
        import inspect

        frame = inspect.currentframe()
        if frame and frame.f_back:
            name = frame.f_back.f_globals.get("__name__", "root")
        else:
            name = "root"

    return structlog.get_logger(name)


# Auto-setup on import if not already configured
if not structlog.is_configured():
    setup_logging()
