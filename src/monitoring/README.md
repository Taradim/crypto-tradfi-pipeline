# Structured Logging

This module provides structured logging using `structlog` with:
- **JSON logs to file** (`logs/pipeline.log`) for querying and analysis
- **Colored console output** for development (always enabled)
- **Context binding** for pipeline tracking

## Quick Start

```python
from src.monitoring import get_logger, bind_context

# Simple logging
logger = get_logger()
logger.info("data_ingested", records_count=150, source="coingecko")

# With context (all logs in the block have pipeline_id and source)
with bind_context(pipeline_id="pipeline_123", source="coingecko"):
    logger.info("starting_ingestion")
    logger.info("data_fetched", records_count=150)
    logger.info("pipeline_completed")
```

## Configuration

Set environment variables in `.env`:

```bash
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

Logs are written to:
- **Console**: Colored, human-readable format
- **File**: `logs/pipeline.log` (JSON format)

## Log Levels

- `DEBUG`: Detailed information for debugging
- `INFO`: General information (default)
- `WARNING`: Warning messages
- `ERROR`: Error messages
- `CRITICAL`: Critical errors

## Context Binding

Context binding automatically adds variables to all logs within a context:

```python
with bind_context(pipeline_id="pipeline_123", source="coingecko"):
    logger.info("event_1")  # Includes pipeline_id and source
    logger.info("event_2")  # Also includes pipeline_id and source
```

## Querying Logs

Since logs are in JSON format, you can query them easily:

```bash
# Find all errors
cat logs/pipeline.log | jq 'select(.level == "ERROR")'

# Find logs for a specific pipeline
cat logs/pipeline.log | jq 'select(.pipeline_id == "pipeline_123")'

# Find logs for a specific source
cat logs/pipeline.log | jq 'select(.source == "coingecko")'
```

## Example Log Output

**Console (colored)**:
```
2025-01-17 12:00:00 [INFO] initializing_s3_structure bucket_name=coding-projects-taradim-2026 layers=['bronze', 'silver', 'gold']
```

**File (JSON)**:
```json
{
  "timestamp": "2025-01-17T12:00:00.123456Z",
  "level": "INFO",
  "event": "initializing_s3_structure",
  "bucket_name": "coding-projects-taradim-2026",
  "layers": ["bronze", "silver", "gold"],
  "pipeline_id": "init_s3_structure"
}
```
