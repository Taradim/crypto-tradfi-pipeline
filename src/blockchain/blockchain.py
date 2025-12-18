"""REST API ingestion example using dlt.

This module is intentionally written with an explicit `main()` entrypoint to avoid
side effects (network calls, local DB writes) when imported.
"""

from __future__ import annotations

import logging
from typing import Any

import dlt
from dlt.sources.rest_api import rest_api_source

logger = logging.getLogger(__name__)


def main() -> Any:
    """Run the REST API ingestion pipeline.

    Returns:
        Load info returned by `pipeline.run(...)`.
    """
    source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com/",
                "auth": {
                    "token": dlt.secrets["your_api_token"],
                },
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "paging.next",
                },
            },
            "resources": [
                # "posts" will be used as the endpoint path, the resource name,
                # and the table name in the destination. The HTTP client will send
                # a request to "https://api.example.com/posts".
                "posts",
                # The explicit configuration allows you to link resources
                # and define query string parameters.
                {
                    "name": "comments",
                    "endpoint": {
                        "path": "posts/{resources.posts.id}/comments",
                        "params": {
                            "sort": "created_at",
                        },
                    },
                },
            ],
        }
    )

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_example",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    logger.info("Starting dlt pipeline run...")
    load_info = pipeline.run(source)
    logger.info("Pipeline run completed.")
    return load_info


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()


