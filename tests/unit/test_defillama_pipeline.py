"""Unit tests for DeFiLlamaPipeline."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.pipelines.defillama import DeFiLlamaPipeline
from tests.fixtures.mock_data import mock_defillama_response


@pytest.mark.unit
class TestDeFiLlamaPipelineExtract:
    """Test DeFiLlamaPipeline.extract() method."""

    @patch("src.pipelines.defillama.requests.Session")
    def test_extract_success_list_response(self, mock_session_class):
        """Test successful extraction with list response."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_data = mock_defillama_response()
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket", endpoint="protocols")
        data = pipeline.extract()

        assert len(data) == len(mock_data)
        assert data[0]["name"] == mock_data[0]["name"]
        assert mock_session.get.call_count == 1

    @patch("src.pipelines.defillama.requests.Session")
    def test_extract_success_dict_response(self, mock_session_class):
        """Test successful extraction with dict response containing protocols key."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_data = {"protocols": mock_defillama_response()}
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket", endpoint="protocols")
        data = pipeline.extract()

        assert len(data) == len(mock_data["protocols"])
        assert data[0]["name"] == mock_data["protocols"][0]["name"]

    @patch("src.pipelines.defillama.requests.Session")
    def test_extract_success_dict_with_tvl(self, mock_session_class):
        """Test successful extraction with dict response containing tvl key."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_data = {"tvl": 1000000.0, "chain": "Ethereum"}
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket", endpoint="tvl")
        data = pipeline.extract()

        assert len(data) == 1
        assert data[0]["tvl"] == 1000000.0

    @patch("src.pipelines.defillama.requests.Session")
    def test_extract_with_api_error(self, mock_session_class):
        """Test extract() when API returns an error."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket")

        with pytest.raises(requests.exceptions.RequestException):
            pipeline.extract()

    @patch("src.pipelines.defillama.requests.Session")
    def test_extract_with_unexpected_format(self, mock_session_class):
        """Test extract() handles unexpected response format."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = "unexpected string format"
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket")
        data = pipeline.extract()

        # Should return empty list for unexpected format
        assert data == []


@pytest.mark.unit
class TestDeFiLlamaPipelineInit:
    """Test DeFiLlamaPipeline.__init__()."""

    @patch("src.pipelines.defillama.requests.Session")
    def test_init_with_api_key(self, mock_session_class, monkeypatch):
        """Test initialization with API key."""
        monkeypatch.setenv("DEFILLAMA_API_KEY", "test-api-key")
        from src.config import Config

        Config.DEFILLAMA_API_KEY = "test-api-key"

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket")

        assert pipeline.api_key == "test-api-key"
        assert pipeline.base_url == "https://api.llama.fi"
        assert pipeline.endpoint == "protocols"  # default
        mock_session.headers.update.assert_called_once()

    @patch("src.pipelines.defillama.requests.Session")
    def test_init_without_api_key(self, mock_session_class, monkeypatch):
        """Test initialization without API key."""
        monkeypatch.setenv("DEFILLAMA_API_KEY", "")
        from src.config import Config

        Config.DEFILLAMA_API_KEY = ""

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket", endpoint="chains")

        assert pipeline.api_key == ""
        assert pipeline.endpoint == "chains"
        mock_session.headers.update.assert_not_called()

    @patch("src.pipelines.defillama.requests.Session")
    def test_init_with_custom_endpoint(self, mock_session_class):
        """Test initialization with custom endpoint."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        pipeline = DeFiLlamaPipeline(bucket_name="test-bucket", endpoint="yields")

        assert pipeline.endpoint == "yields"
        assert pipeline.base_url == "https://api.llama.fi"
