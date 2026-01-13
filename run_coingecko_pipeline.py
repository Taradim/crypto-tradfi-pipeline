"""Execute the complete CoinGecko pipeline (extract → transform → load)."""

import logging

from src.config import Config
from src.monitoring import setup_logging
from src.pipelines.coingecko import CoinGeckoPipeline

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Validate configuration
Config.validate()

# Create and execute pipeline
logger.info("Starting CoinGecko pipeline...")
pipeline = CoinGeckoPipeline()

try:
    s3_path = pipeline.run()
    logger.info("Pipeline completed successfully")
    logger.info(f"File uploaded to: s3://{Config.S3_BUCKET_NAME}/{s3_path}")
except Exception as e:
    logger.error(f"Error during pipeline execution: {e}", exc_info=True)
    raise
