"""Unit tests for YahooFinancePipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipelines.yahoo_finance import YahooFinancePipeline


@pytest.mark.unit
class TestYahooFinancePipelineExtract:
    """Test YahooFinancePipeline.extract() method."""

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_extract_success_single_symbol(self, mock_ticker_class):
        """Test successful extraction with single symbol."""
        # Create mock DataFrame
        mock_df = pd.DataFrame(
            {
                "Date": pd.date_range("2026-01-01", periods=5, freq="D"),
                "Open": [100.0, 101.0, 102.0, 103.0, 104.0],
                "High": [105.0, 106.0, 107.0, 108.0, 109.0],
                "Low": [99.0, 100.0, 101.0, 102.0, 103.0],
                "Close": [104.0, 105.0, 106.0, 107.0, 108.0],
                "Volume": [1000000, 1100000, 1200000, 1300000, 1400000],
            }
        )
        mock_df.set_index("Date", inplace=True)

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        pipeline = YahooFinancePipeline(
            bucket_name="test-bucket", symbols=["AAPL"], period="5d", interval="1d"
        )
        df = pipeline.extract()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "Symbol" in df.columns
        assert df["Symbol"].iloc[0] == "AAPL"
        assert "Date" in df.columns
        mock_ticker.history.assert_called_once_with(period="5d", interval="1d")

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_extract_success_multiple_symbols(self, mock_ticker_class):
        """Test successful extraction with multiple symbols."""
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

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        pipeline = YahooFinancePipeline(
            bucket_name="test-bucket",
            symbols=["AAPL", "GOOGL", "MSFT"],
            period="3d",
            interval="1d",
        )
        df = pipeline.extract()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 9  # 3 symbols × 3 days
        assert df["Symbol"].nunique() == 3
        assert set(df["Symbol"].unique()) == {"AAPL", "GOOGL", "MSFT"}
        assert mock_ticker_class.call_count == 3

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_extract_with_empty_data(self, mock_ticker_class):
        """Test extract() when symbol returns empty data."""
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()  # Empty DataFrame
        mock_ticker_class.return_value = mock_ticker

        pipeline = YahooFinancePipeline(
            bucket_name="test-bucket", symbols=["INVALID"], period="1d", interval="1d"
        )

        # Should raise ValueError because no data extracted
        with pytest.raises(ValueError, match="No data extracted"):
            pipeline.extract()

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_extract_with_partial_failure(self, mock_ticker_class):
        """Test extract() when some symbols fail but others succeed."""
        # First symbol succeeds
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

        # Second symbol fails
        mock_ticker_success = MagicMock()
        mock_ticker_success.history.return_value = mock_df_success

        mock_ticker_fail = MagicMock()
        mock_ticker_fail.history.side_effect = Exception("API Error")

        # Return success for first call, fail for second
        mock_ticker_class.side_effect = [mock_ticker_success, mock_ticker_fail]

        pipeline = YahooFinancePipeline(
            bucket_name="test-bucket", symbols=["AAPL", "INVALID"], period="2d", interval="1d"
        )
        df = pipeline.extract()

        # Should succeed with data from first symbol only
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df["Symbol"].iloc[0] == "AAPL"

    @patch("src.pipelines.yahoo_finance.yf.Ticker")
    def test_extract_with_default_symbols(self, mock_ticker_class):
        """Test extract() uses default symbols when none provided."""
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

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        pipeline = YahooFinancePipeline(bucket_name="test-bucket")
        df = pipeline.extract()

        # Should use default symbols: ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        assert mock_ticker_class.call_count == 5
        assert len(df) == 10  # 5 symbols × 2 days


@pytest.mark.unit
class TestYahooFinancePipelineInit:
    """Test YahooFinancePipeline.__init__()."""

    def test_init_with_custom_symbols(self):
        """Test initialization with custom symbols."""
        pipeline = YahooFinancePipeline(
            bucket_name="test-bucket", symbols=["AAPL", "TSLA"], period="1mo", interval="1d"
        )

        assert pipeline.symbols == ["AAPL", "TSLA"]
        assert pipeline.period == "1mo"
        assert pipeline.interval == "1d"
        assert pipeline.source_name == "yahoo_finance"

    def test_init_with_default_symbols(self):
        """Test initialization with default symbols."""
        pipeline = YahooFinancePipeline(bucket_name="test-bucket")

        assert pipeline.symbols == ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        assert pipeline.period == "1mo"
        assert pipeline.interval == "1d"

    def test_init_with_custom_period_and_interval(self):
        """Test initialization with custom period and interval."""
        pipeline = YahooFinancePipeline(
            bucket_name="test-bucket", period="1y", interval="1wk"
        )

        assert pipeline.period == "1y"
        assert pipeline.interval == "1wk"
