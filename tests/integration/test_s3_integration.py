"""Integration tests for S3 operations using moto."""

import io

import pandas as pd
import pytest
from moto import mock_aws

from src.pipelines.base import BasePipeline
from tests.fixtures.mock_data import mock_dataframe


class MockPipeline(BasePipeline):
    """Mock pipeline for S3 integration testing."""

    def extract(self):
        """Return mock data."""
        return [{"id": "test", "value": 123}]


@pytest.mark.integration
@mock_aws
class TestS3Integration:
    """Test S3 integration with moto (realistic S3 behavior)."""

    def test_s3_bucket_creation(self, mock_s3_bucket):
        """Test that mock S3 bucket is created correctly."""
        import boto3

        s3_client = boto3.client("s3", region_name="eu-west-1")
        response = s3_client.list_buckets()

        bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
        assert mock_s3_bucket in bucket_names

    def test_s3_upload_file(self, mock_s3_bucket, mock_s3_client):
        """Test uploading a file to mock S3."""
        df = mock_dataframe()
        pipeline = MockPipeline("test", bucket_name=mock_s3_bucket)

        s3_path = pipeline.load_to_s3(df, layer="bronze", format="parquet")

        # Verify file exists in S3
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert "Contents" in response
        assert len(response["Contents"]) == 1
        assert response["Contents"][0]["Key"] == s3_path

    def test_s3_upload_and_download(self, mock_s3_bucket, mock_s3_client):
        """Test uploading and downloading a file from mock S3."""
        df = mock_dataframe()
        pipeline = MockPipeline("test", bucket_name=mock_s3_bucket)

        # Upload
        s3_path = pipeline.load_to_s3(df, layer="bronze", format="parquet")

        # Download and verify content
        response = mock_s3_client.get_object(Bucket=mock_s3_bucket, Key=s3_path)
        downloaded_data = response["Body"].read()

        # Read parquet from bytes
        downloaded_df = pd.read_parquet(io.BytesIO(downloaded_data))

        assert len(downloaded_df) == len(df)
        assert list(downloaded_df.columns) == list(df.columns)
        assert downloaded_df.iloc[0]["id"] == df.iloc[0]["id"]

    def test_s3_upload_json_format(self, mock_s3_bucket, mock_s3_client):
        """Test uploading JSON format to S3."""
        df = mock_dataframe()
        pipeline = MockPipeline("test", bucket_name=mock_s3_bucket)

        s3_path = pipeline.load_to_s3(df, layer="bronze", format="json")

        # Verify file exists
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert response["Contents"][0]["Key"] == s3_path
        assert s3_path.endswith(".json")

    def test_s3_path_structure(self, mock_s3_bucket, mock_s3_client):
        """Test that S3 path follows the correct structure."""
        df = mock_dataframe()
        pipeline = MockPipeline("test", bucket_name=mock_s3_bucket)

        s3_path = pipeline.load_to_s3(df, layer="silver", format="parquet")

        # Verify path structure: layer/source/date/filename
        parts = s3_path.split("/")
        assert parts[0] == "silver"
        assert parts[1] == "test"
        assert len(parts) == 4  # layer/source/date/filename

    def test_s3_multiple_uploads(self, mock_s3_bucket, mock_s3_client):
        """Test uploading multiple files to S3."""
        df = mock_dataframe()
        pipeline = MockPipeline("test", bucket_name=mock_s3_bucket)

        # Upload multiple files
        path1 = pipeline.load_to_s3(df, layer="bronze", format="parquet")
        path2 = pipeline.load_to_s3(df, layer="silver", format="parquet")

        # Verify both files exist
        response = mock_s3_client.list_objects_v2(Bucket=mock_s3_bucket)
        assert len(response["Contents"]) == 2

        keys = [obj["Key"] for obj in response["Contents"]]
        assert path1 in keys
        assert path2 in keys

    def test_s3_parquet_preserves_complex_types(self, mock_s3_bucket, mock_s3_client):
        """Test that Parquet files handle complex types (lists, dicts).

        When PyArrow can handle them natively, they're preserved.
        Otherwise, they're converted to JSON strings for compatibility.
        """
        import json

        import pyarrow.parquet as pq

        # Create DataFrame with complex nested structures
        data = [
            {
                "id": "1",
                "name": "Test Protocol",
                "chains": ["Ethereum", "Arbitrum", "Base"],  # List
                "chainTvls": {  # Dict
                    "Ethereum": 1000000.0,
                    "Arbitrum": 500000.0,
                },
                "hallmarks": [  # List of lists
                    [1650471689, "Event 1"],
                    [1659630089, "Event 2"],
                ],
            }
        ]
        df = pd.DataFrame(data)
        pipeline = MockPipeline("test", bucket_name=mock_s3_bucket)

        # Upload to S3
        s3_path = pipeline.load_to_s3(df, layer="bronze", format="parquet")

        # Download and read back
        response = mock_s3_client.get_object(Bucket=mock_s3_bucket, Key=s3_path)
        downloaded_data = response["Body"].read()

        # Read with PyArrow
        table = pq.read_table(io.BytesIO(downloaded_data))
        downloaded_df = table.to_pandas()

        # Complex types may be preserved natively OR converted to JSON strings
        # depending on PyArrow's ability to handle them
        chains_value = downloaded_df["chains"].iloc[0]
        if isinstance(chains_value, list):
            # Native PyArrow handling
            assert chains_value == ["Ethereum", "Arbitrum", "Base"]
        else:
            # JSON fallback (expected for mixed/complex types)
            assert isinstance(chains_value, str)
            parsed = json.loads(chains_value)
            assert parsed == ["Ethereum", "Arbitrum", "Base"]

        # Verify data integrity - can be parsed back from JSON if needed
        chain_tvls = downloaded_df["chainTvls"].iloc[0]
        if isinstance(chain_tvls, dict):
            # Native handling
            assert chain_tvls["Ethereum"] == 1000000.0
        else:
            # JSON fallback
            assert isinstance(chain_tvls, str)
            parsed = json.loads(chain_tvls)
            assert parsed["Ethereum"] == 1000000.0
            assert parsed["Arbitrum"] == 500000.0
