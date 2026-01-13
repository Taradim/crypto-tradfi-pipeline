"""Shared pytest fixtures for integration tests."""

from collections.abc import Generator

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def mock_s3_bucket(monkeypatch: pytest.MonkeyPatch) -> Generator[str, None, None]:
    """Create a mock S3 bucket for integration tests.

    This fixture:
    - Creates a mock S3 bucket using moto
    - Sets environment variables for the pipeline
    - Returns the bucket name

    Note: This fixture uses its own mock_aws context manager because fixtures
    execute in their own scope, independent of class decorators. The @mock_aws
    decorator on test classes provides additional mocking coverage for the test
    methods themselves.

    Yields:
        str: Name of the created mock S3 bucket
    """
    with mock_aws():
        bucket_name = "test-integration-bucket"
        region = "eu-west-1"

        # Create mock S3 bucket
        s3_client = boto3.client("s3", region_name=region)
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

        # Set environment variables for the pipeline
        monkeypatch.setenv("S3_BUCKET_NAME", bucket_name)
        monkeypatch.setenv("AWS_DEFAULT_REGION", region)

        yield bucket_name

        # Cleanup is automatic with moto context manager


@pytest.fixture
def mock_s3_client(mock_s3_bucket):
    """Get a boto3 S3 client for the mock bucket.

    Args:
        mock_s3_bucket: Mock S3 bucket fixture

    Note: This fixture uses its own mock_aws context manager to ensure
    the client is created within the mocked AWS environment.

    Yields:
        boto3.client: S3 client connected to mock bucket
    """
    with mock_aws():
        region = "eu-west-1"
        client = boto3.client("s3", region_name=region)
        yield client
