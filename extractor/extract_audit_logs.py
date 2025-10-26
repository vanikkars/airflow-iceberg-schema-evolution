#!/usr/bin/env python3
"""
Standalone extraction script for audit logs.
Runs in a Docker container to extract data from PostgreSQL and upload to S3.
Uses lightweight hook-like wrappers around boto3 and psycopg2.
"""

import csv
import os
import sys
import logging
import argparse
import json
from io import StringIO
from datetime import datetime
import pendulum
import psycopg2
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
PG_HOST = os.getenv('PG_HOST', 'ecommerce-db')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'ecom')
PG_USER = os.getenv('PG_USER', 'ecom')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'ecom')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'lake')
S3_PREFIX = os.getenv('S3_PREFIX', 'raw/ecommerce/orders')

EXTRACT_BATCH_SIZE = int(os.getenv('EXTRACT_BATCH_SIZE', '100'))


class PostgresHandler:
    """
    Lightweight PostgreSQL hook wrapper for standalone scripts.
    Provides a clean interface around psycopg2.
    """

    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._conn = None

    def get_conn(self):
        """Get or create database connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
        return self._conn

    def get_records(self, sql, parameters=None):
        """
        Execute SQL query and return all records.

        Args:
            sql: SQL query string
            parameters: Query parameters (tuple)

        Returns:
            List of tuples (query results)
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, parameters)
            return cursor.fetchall()
        finally:
            cursor.close()

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Closed PostgreSQL connection")


class S3Handler:
    """
    Lightweight S3 hook wrapper for standalone scripts.
    Provides a clean interface around boto3.
    """

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


def get_postgres_handler():
    """Create PostgreSQL handler from environment variables."""
    return PostgresHandler(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )


def get_s3_handler():
    """Create S3 handler from environment variables."""
    return S3Handler(
        endpoint_url=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY
    )


def extract_audit_logs(data_interval_start: str, data_interval_end: str):
    """
    Extract audit logs from PostgreSQL and upload to S3.

    Args:
        data_interval_start: Start of data interval (ISO format)
        data_interval_end: End of data interval (ISO format)

    Returns:
        List of S3 keys for uploaded files
    """
    logger.info(f"Starting extraction for interval: {data_interval_start} to {data_interval_end}")

    # Parse timestamps
    start_dt = pendulum.parse(data_interval_start)
    end_dt = pendulum.parse(data_interval_end)

    # Get handlers
    pg_handler = get_postgres_handler()
    s3_handler = get_s3_handler()

    sql = """
        SELECT audit_event_id,
               audit_operation,
               audit_timestamp,
               tbl_schema,
               tbl_name,
               raw_data
        FROM audit_log_dml
        WHERE audit_timestamp >= %s AND audit_timestamp < %s
        ORDER BY audit_timestamp
        LIMIT %s OFFSET %s
    """

    extracted_files = []
    offset = 0
    iteration = 0
    batch_size = EXTRACT_BATCH_SIZE

    try:
        while True:
            iteration += 1
            logger.info('=' * 100)
            logger.info(f'Iteration: {iteration}')

            # Execute query using PostgresHandler
            rows = pg_handler.get_records(
                sql=sql,
                parameters=(start_dt, end_dt, batch_size, offset)
            )

            if not rows:
                logger.info('No more rows to fetch, exiting loop.')
                break

            # Create CSV buffer
            header = ["audit_event_id", "audit_operation", "audit_timestamp", "tbl_schema", "tbl_name", "raw_data"]
            csv_buffer = StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerow(header)
            csv_writer.writerows(rows)

            # Generate S3 key with partition path
            s3_key = (
                f"{S3_PREFIX}/{end_dt.strftime('%Y/%m/%d')}/audit_log_{start_dt.isoformat()}_{end_dt.isoformat()}_{batch_size}_{offset}.csv"
            ).replace(":", "-")

            # Upload to S3 using S3Handler
            try:
                s3_handler.load_string(
                    string_data=csv_buffer.getvalue(),
                    key=s3_key,
                    bucket_name=S3_BUCKET_NAME,
                    replace=True
                )
                extracted_files.append(s3_key)
                logger.info(
                    f"Iteration={iteration} Wrote {len(rows)} records to s3://{S3_BUCKET_NAME}/{s3_key}, "
                    f"batch_size={batch_size}, offset={offset}"
                )
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
                raise

            offset += batch_size

        logger.info(f"Extraction complete. Created {len(extracted_files)} files.")
        return extracted_files

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        # Clean up database connection
        pg_handler.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Extract audit logs from PostgreSQL and upload to S3',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--data-interval-start',
        required=True,
        help='Start of data interval in ISO format (e.g., 2024-01-01T00:00:00+00:00)'
    )

    parser.add_argument(
        '--data-interval-end',
        required=True,
        help='End of data interval in ISO format (e.g., 2024-01-02T00:00:00+00:00)'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    try:
        extracted_files = extract_audit_logs(args.data_interval_start, args.data_interval_end)
        logger.info(f"Successfully extracted {len(extracted_files)} files")

        # Output result as JSON to stdout for XCom capture
        result = {
            'extracted_files': extracted_files,
            'count': len(extracted_files)
        }
        print(json.dumps(result))
        sys.exit(0)
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
