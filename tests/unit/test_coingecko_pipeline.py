"""Unit tests for CoinGeckoPipeline."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.pipelines.coingecko import CoinGeckoPipeline
from tests.fixtures.mock_data import mock_coingecko_response


@pytest.mark.unit
class TestCoinGeckoPipelineExtract:
    """Test CoinGeckoPipeline.extract() method."""

    @patch("src.pipelines.coingecko.requests.Session")
    def test_extract_success_single_page(self, mock_session_class):
        """Test successful extraction of single page."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        # Return only first 2 records for single page test
        mock_data = mock_coingecko_response()[:2]
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Test with num_pages=1 for single page
        pipeline = CoinGeckoPipeline(bucket_name="test-bucket", num_pages=1)
        data = pipeline.extract()

        assert len(data) == 2  # 1 page × 2 records
        assert data[0]["id"] == mock_data[0]["id"]
        assert data[1]["id"] == mock_data[1]["id"]
        assert mock_session.get.call_count == 1  # 1 page only

    @patch("src.pipelines.coingecko.requests.Session")
    def test_extract_with_api_error(self, mock_session_class):
        """Test extract() when API returns an error."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = CoinGeckoPipeline(bucket_name="test-bucket")

        with pytest.raises(requests.exceptions.RequestException):
            pipeline.extract()

    @patch("src.pipelines.coingecko.requests.Session")
    def test_extract_with_rate_limit(self, mock_session_class):
        """Test extract() handles rate limiting."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "429 Too Many Requests"
        )
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = CoinGeckoPipeline(bucket_name="test-bucket")

        with pytest.raises(requests.exceptions.RequestException):
            pipeline.extract()


@pytest.mark.unit
class TestCoinGeckoPipelineInit:
    """Test CoinGeckoPipeline.__init__()."""

    @patch("src.pipelines.coingecko.requests.Session")
    def test_init_with_api_key(self, mock_session_class, monkeypatch):
        """Test initialization with API key."""
        monkeypatch.setenv("COINGECKO_API_KEY", "test-api-key")
        from src.config import Config

        Config.COINGECKO_API_KEY = "test-api-key"

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        pipeline = CoinGeckoPipeline(bucket_name="test-bucket")

        assert pipeline.api_key == "test-api-key"
        assert pipeline.base_url == "https://api.coingecko.com/api/v3"
        mock_session.headers.update.assert_called_once()

    @patch("src.pipelines.coingecko.requests.Session")
    def test_init_without_api_key(self, mock_session_class, monkeypatch):
        """Test initialization without API key."""
        monkeypatch.setenv("COINGECKO_API_KEY", "")
        from src.config import Config

        Config.COINGECKO_API_KEY = ""

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        pipeline = CoinGeckoPipeline(bucket_name="test-bucket")

        assert pipeline.api_key == ""
        mock_session.headers.update.assert_not_called()
