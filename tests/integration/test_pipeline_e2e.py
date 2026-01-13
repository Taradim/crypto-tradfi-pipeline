"""End-to-end integration tests for complete pipeline execution."""

from unittest.mock import patch

import pytest
from moto import mock_aws

from src.pipelines.coingecko import CoinGeckoPipeline
from tests.fixtures.mock_data import mock_coingecko_response


@pytest.mark.integration
@mock_aws
class TestPipelineE2E:
    """Test complete pipeline execution end-to-end."""

    @patch("src.pipelines.coingecko.requests.Session")
    def test_pipeline_e2e_with_mocked_api(self, mock_session_class, mock_s3_bucket, mock_s3_client):
        """Test complete pipeline with mocked API and real S3 operations."""
        # Setup mocked API response
        mock_session = type("Session", (), {})()
        mock_response = type("Response", (), {})()
        mock_response.status_code = 200
        mock_response.json = lambda: mock_coingecko_response()[:5]  # 5 records
        mock_response.raise_for_status = lambda: None

        def mock_get(*args, **kwargs):
            return mock_response

        mock_session.get = mock_get
        mock_session.headers = {}
        mock_session.mount = lambda *args: None

        mock_session_class.return_value = mock_session

        # Execute pipeline
        pipeline = CoinGeckoPipeline(bucket_name=mock_s3_bucket, num_pages=1)
        s3_path = pipeline.run()

        # Verify file was uploaded to S3
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert "Contents" in response
        assert len(response["Contents"]) == 1
        assert response["Contents"][0]["Key"] == s3_path

        # Verify file content
        file_response = mock_s3_client.get_object(Bucket=mock_s3_bucket, Key=s3_path)
        assert file_response["ContentType"] == "application/parquet"

    @patch("src.pipelines.coingecko.requests.Session")
    def test_pipeline_e2e_multiple_pages(self, mock_session_class, mock_s3_bucket, mock_s3_client):
        """Test pipeline with multiple pages."""
        # Setup mocked API response
        mock_session = type("Session", (), {})()
        mock_response = type("Response", (), {})()
        mock_response.status_code = 200
        mock_response.json = lambda: mock_coingecko_response()[:3]  # 3 records per page
        mock_response.raise_for_status = lambda: None

        def mock_get(*args, **kwargs):
            return mock_response

        mock_session.get = mock_get
        mock_session.headers = {}
        mock_session.mount = lambda *args: None

        mock_session_class.return_value = mock_session

        # Execute pipeline with 2 pages
        pipeline = CoinGeckoPipeline(bucket_name=mock_s3_bucket, num_pages=2)
        pipeline.run()

        # Verify file exists
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert len(response["Contents"]) == 1

        # Verify pipeline_id was set
        assert pipeline.pipeline_id is not None
        assert "coingecko" in pipeline.pipeline_id

    def test_pipeline_e2e_error_handling(self, mock_s3_bucket, mock_s3_client):
        """Test pipeline error handling end-to-end."""

        # Create a pipeline that will fail during extract
        class FailingPipeline(CoinGeckoPipeline):
            def extract(self):
                raise ValueError("API connection failed")

        pipeline = FailingPipeline(bucket_name=mock_s3_bucket)

        # Verify error is raised and nothing is uploaded
        with pytest.raises(ValueError, match="API connection failed"):
            pipeline.run()

        # Verify no file was uploaded
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert "Contents" not in response or len(response.get("Contents", [])) == 0
