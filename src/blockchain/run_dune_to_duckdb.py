"""Run the Dune -> dlt -> DuckDB pipeline.

Usage (recommended):
    PYTHONPATH=src python -m blockchain.run_dune_to_duckdb

Fallback (works without PYTHONPATH thanks to a small sys.path shim):
    python src/blockchain/run_dune_to_duckdb.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import dlt


def _ensure_src_on_syspath() -> None:
    """Allow running this file as a script without installing the package."""

    here = Path(__file__).resolve()
    src_dir = here.parents[1]  # .../src
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def main(argv: list[str] | None = None) -> Any:
    """Run the pipeline and return dlt load info."""

    _ensure_src_on_syspath()

    # Imported after sys.path fix.
    from blockchain.dune_dlt_source import dune_source

    parser = argparse.ArgumentParser(description="Run Dune -> dlt -> DuckDB pipeline.")
    parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help="Path to `.dlt/config.toml` (defaults to project-root `.dlt/config.toml`).",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="dune_queries",
        help="TOML namespace to read (default: dune_queries).",
    )
    parser.add_argument(
        "--pipeline-name",
        type=str,
        default="dune_source",
        help="dlt pipeline name (default: dune_source).",
    )
    parser.add_argument(
        "--dataset-name",
        type=str,
        default="dune_queries",
        help="dlt dataset name / schema (default: dune_queries).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level))

    pipeline = dlt.pipeline(
        pipeline_name=args.pipeline_name,
        destination="duckdb",
        dataset_name=args.dataset_name,
    )

    load_info = pipeline.run(
        dune_source(config_path=args.config_path, namespace=args.namespace)
    )
    logging.getLogger(__name__).info("dlt load completed: %s", load_info)
    return load_info


if __name__ == "__main__":
    main()


