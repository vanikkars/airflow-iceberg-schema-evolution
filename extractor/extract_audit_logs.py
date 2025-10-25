#!/usr/bin/env python3
"""
Standalone extraction script for audit logs.
Runs in a Docker container to extract data from PostgreSQL and upload to S3.
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

# Third-party imports
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


def get_postgres_connection():
    """Create PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def get_s3_client():
    """Create S3 client."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
        return s3_client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        raise


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

    # Get connections
    conn = get_postgres_connection()
    s3_client = get_s3_client()

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
        cursor = conn.cursor()

        while True:
            iteration += 1
            logger.info('=' * 100)
            logger.info(f'Iteration: {iteration}')

            # Execute query
            cursor.execute(sql, (start_dt, end_dt, batch_size, offset))
            rows = cursor.fetchall()

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

            # Upload to S3
            try:
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=csv_buffer.getvalue().encode('utf-8')
                )
                extracted_files.append(s3_key)
                logger.info(
                    f"Iteration={iteration} Wrote {len(rows)} records to s3://{S3_BUCKET_NAME}/{s3_key}, "
                    f"batch_size={batch_size}, offset={offset}"
                )
            except ClientError as e:
                logger.error(f"Failed to upload to S3: {e}")
                raise

            offset += batch_size

        cursor.close()
        conn.close()

        logger.info(f"Extraction complete. Created {len(extracted_files)} files.")
        return extracted_files

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        if conn:
            conn.close()
        raise


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
