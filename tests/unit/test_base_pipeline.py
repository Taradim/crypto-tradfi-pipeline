"""Unit tests for BasePipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from src.pipelines.base import BasePipeline
from tests.fixtures.mock_data import mock_dataframe


class MockPipeline(BasePipeline):
    """Mock implementation of BasePipeline for testing."""

    def extract(self):
        """Mock extract method."""
        return [{"id": "test", "value": 123}]


@pytest.mark.unit
class TestBasePipelineTransform:
    """Test BasePipeline.transform() method."""

    def test_transform_with_dataframe(self):
        """Test transform with DataFrame input."""
        pipeline = MockPipeline("test", bucket_name="test-bucket")
        df = pd.DataFrame([{"id": "test", "value": 123}])
        result = pipeline.transform(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["id"] == "test"

    def test_transform_with_dict(self):
        """Test transform with dict input."""
        pipeline = MockPipeline("test", bucket_name="test-bucket")
        data = {"id": "test", "value": 123}
        result = pipeline.transform(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["id"] == "test"

    def test_transform_with_list(self):
        """Test transform with list of dicts input."""
        pipeline = MockPipeline("test", bucket_name="test-bucket")
        data = [{"id": "test1", "value": 123}, {"id": "test2", "value": 456}]
        result = pipeline.transform(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0]["id"] == "test1"
        assert result.iloc[1]["id"] == "test2"

    def test_transform_with_unsupported_type(self):
        """Test transform with unsupported data type."""
        pipeline = MockPipeline("test", bucket_name="test-bucket")
        with pytest.raises(ValueError, match="Unsupported data type"):
            pipeline.transform("invalid")  # type: ignore[arg-type]


@pytest.mark.unit
class TestBasePipelineLoadToS3:
    """Test BasePipeline.load_to_s3() method."""

    @patch("src.pipelines.base.boto3.client")
    def test_load_to_s3_parquet_success(self, mock_boto3_client):
        """Test successful upload to S3 in parquet format."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        pipeline = MockPipeline("test", bucket_name="test-bucket")
        df = mock_dataframe()

        s3_path = pipeline.load_to_s3(df, layer="bronze", format="parquet")

        assert s3_path.startswith("bronze/test/")
        assert s3_path.endswith(".parquet")
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["ContentType"] == "application/parquet"

    @patch("src.pipelines.base.boto3.client")
    def test_load_to_s3_json_success(self, mock_boto3_client):
        """Test successful upload to S3 in JSON format."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        pipeline = MockPipeline("test", bucket_name="test-bucket")
        df = mock_dataframe()

        s3_path = pipeline.load_to_s3(df, layer="bronze", format="json")

        assert s3_path.startswith("bronze/test/")
        assert s3_path.endswith(".json")
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]["ContentType"] == "application/json"

    @patch("src.pipelines.base.boto3.client")
    def test_load_to_s3_unsupported_format(self, mock_boto3_client):
        """Test load_to_s3 with unsupported format."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        pipeline = MockPipeline("test", bucket_name="test-bucket")
        df = mock_dataframe()

        with pytest.raises(ValueError, match="Unsupported format"):
            pipeline.load_to_s3(df, format="csv")  # type: ignore[arg-type]

    @patch("src.pipelines.base.boto3.client")
    def test_load_to_s3_client_error(self, mock_boto3_client):
        """Test load_to_s3 with S3 client error."""
        mock_s3_client = MagicMock()
        mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )
        mock_boto3_client.return_value = mock_s3_client

        pipeline = MockPipeline("test", bucket_name="test-bucket")
        df = mock_dataframe()

        with pytest.raises(ClientError):
            pipeline.load_to_s3(df)


@pytest.mark.unit
class TestBasePipelineRun:
    """Test BasePipeline.run() method."""

    @patch("src.pipelines.base.boto3.client")
    def test_run_complete_pipeline(self, mock_boto3_client):
        """Test complete pipeline execution."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        pipeline = MockPipeline("test", bucket_name="test-bucket")

        s3_path = pipeline.run()

        assert s3_path is not None
        assert pipeline.pipeline_id is not None
        mock_s3_client.put_object.assert_called_once()

    @patch("src.pipelines.base.boto3.client")
    def test_run_with_extract_error(self, mock_boto3_client):
        """Test run() when extract() raises an error."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        class FailingPipeline(BasePipeline):
            def extract(self):
                raise ValueError("Extraction failed")

        pipeline = FailingPipeline("test", bucket_name="test-bucket")

        with pytest.raises(ValueError, match="Extraction failed"):
            pipeline.run()
