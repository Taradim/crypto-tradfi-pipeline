"""End-to-end integration tests for Yahoo Finance pipeline execution."""

from unittest.mock import patch

import pandas as pd
import pytest
from moto import mock_aws

from src.pipelines.yahoo_finance import YahooFinancePipeline


@pytest.mark.integration
@mock_aws
class TestYahooFinancePipelineE2E:
    """Test complete Yahoo Finance pipeline execution end-to-end."""

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_pipeline_e2e_with_mocked_data(self, mock_ticker_class, mock_s3_bucket, mock_s3_client):
        """Test complete pipeline with mocked yfinance and real S3 operations."""
        # Create mock DataFrame
        mock_df = pd.DataFrame(
            {
                "Date": pd.date_range("2026-01-01", periods=3, freq="D"),
                "Open": [100.0, 101.0, 102.0],
                "High": [105.0, 106.0, 107.0],
                "Low": [99.0, 100.0, 101.0],
                "Close": [104.0, 105.0, 106.0],
                "Volume": [1000000, 1100000, 1200000],
            }
        )
        mock_df.set_index("Date", inplace=True)

        mock_ticker = type("Ticker", (), {})()
        mock_ticker.history = lambda **kwargs: mock_df
        mock_ticker_class.return_value = mock_ticker

        # Execute pipeline
        pipeline = YahooFinancePipeline(
            bucket_name=mock_s3_bucket, symbols=["AAPL", "GOOGL"], period="3d", interval="1d"
        )
        s3_path = pipeline.run()

        # Verify file was uploaded to S3
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert "Contents" in response
        assert len(response["Contents"]) == 1
        assert response["Contents"][0]["Key"] == s3_path

        # Verify file content
        file_response = mock_s3_client.get_object(Bucket=mock_s3_bucket, Key=s3_path)
        assert file_response["ContentType"] == "application/parquet"

        # Verify pipeline_id was set
        assert pipeline.pipeline_id is not None
        assert "yahoo_finance" in pipeline.pipeline_id

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_pipeline_e2e_multiple_symbols(
        self, mock_ticker_class, mock_s3_bucket, mock_s3_client
    ):
        """Test pipeline with multiple symbols."""
        # Create mock DataFrame
        mock_df = pd.DataFrame(
            {
                "Date": pd.date_range("2026-01-01", periods=2, freq="D"),
                "Open": [100.0, 101.0],
                "High": [105.0, 106.0],
                "Low": [99.0, 100.0],
                "Close": [104.0, 105.0],
                "Volume": [1000000, 1100000],
            }
        )
        mock_df.set_index("Date", inplace=True)

        mock_ticker = type("Ticker", (), {})()
        mock_ticker.history = lambda **kwargs: mock_df
        mock_ticker_class.return_value = mock_ticker

        # Execute pipeline with 3 symbols
        pipeline = YahooFinancePipeline(
            bucket_name=mock_s3_bucket,
            symbols=["AAPL", "GOOGL", "MSFT"],
            period="2d",
            interval="1d",
        )
        pipeline.run()

        # Verify file exists
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert len(response["Contents"]) == 1

        # Verify that yf.Ticker was called 3 times (once per symbol)
        assert mock_ticker_class.call_count == 3

    def test_pipeline_e2e_error_handling(self, mock_s3_bucket, mock_s3_client):
        """Test pipeline error handling end-to-end."""

        # Create a pipeline that will fail during extract
        class FailingPipeline(YahooFinancePipeline):
            def extract(self):
                raise ValueError("Data extraction failed")

        pipeline = FailingPipeline(bucket_name=mock_s3_bucket)

        # Verify error is raised and nothing is uploaded
        with pytest.raises(ValueError, match="Data extraction failed"):
            pipeline.run()

        # Verify no file was uploaded
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert "Contents" not in response or len(response.get("Contents", [])) == 0

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_pipeline_e2e_with_partial_failure(
        self, mock_ticker_class, mock_s3_bucket, mock_s3_client
    ):
        """Test pipeline when some symbols fail but others succeed."""
        # Create mock DataFrame for successful symbol
        mock_df_success = pd.DataFrame(
            {
                "Date": pd.date_range("2026-01-01", periods=2, freq="D"),
                "Open": [100.0, 101.0],
                "High": [105.0, 106.0],
                "Low": [99.0, 100.0],
                "Close": [104.0, 105.0],
                "Volume": [1000000, 1100000],
            }
        )
        mock_df_success.set_index("Date", inplace=True)

        # Create mock tickers: first succeeds, second fails
        mock_ticker_success = type("Ticker", (), {})()
        mock_ticker_success.history = lambda **kwargs: mock_df_success

        mock_ticker_fail = type("Ticker", (), {})()
        mock_ticker_fail.history = lambda **kwargs: (_ for _ in ()).throw(Exception("API Error"))

        mock_ticker_class.side_effect = [mock_ticker_success, mock_ticker_fail]

        # Execute pipeline - should succeed with data from first symbol only
        pipeline = YahooFinancePipeline(
            bucket_name=mock_s3_bucket, symbols=["AAPL", "INVALID"], period="2d", interval="1d"
        )
        s3_path = pipeline.run()

        # Verify file was uploaded (with data from successful symbol)
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert len(response["Contents"]) == 1
        assert response["Contents"][0]["Key"] == s3_path
