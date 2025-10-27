"""
S3 handler for reading and writing files to S3/MinIO.
"""

import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Handler:
    """Lightweight S3 handler for file operations."""

    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self._client = None

    def get_client(self):
        """Get or create S3 client."""
        if self._client is None:
            self._client = boto3.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
            )
            logger.info(f"Created S3 client for endpoint: {self.endpoint_url}")
        return self._client

    def load_string(self, string_data, key, bucket_name, replace=True):
        """
        Upload string data to S3.

        Args:
            string_data: String content to upload
            key: S3 object key (path)
            bucket_name: S3 bucket name
            replace: Whether to replace existing object (unused, always replaces)
        """
        client = self.get_client()
        try:
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=string_data.encode('utf-8')
            )
            logger.debug(f"Uploaded data to s3://{bucket_name}/{key}")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise

    def read_key(self, key, bucket_name):
        """
        Read a file from S3 as a string.

        Args:
            key: S3 object key (path)
            bucket_name: S3 bucket name

        Returns:
            File content as string
        """
        client = self.get_client()
        try:
            response = client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            logger.info(f"Read {len(content)} bytes from s3://{bucket_name}/{key}")
            return content
        except ClientError as e:
            logger.error(f"Failed to read from S3: {e}")
            raise
