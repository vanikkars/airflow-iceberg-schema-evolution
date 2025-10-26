#!/usr/bin/env python3
"""
Standalone ingestion script for loading data into Iceberg via Trino.
Runs in a Docker container to read CSV files from S3 and insert into Iceberg tables.
Uses lightweight wrappers around boto3 and trino-python-client.
"""

import csv
import os
import sys
import logging
import argparse
import json
from io import StringIO

import pendulum
import boto3
from botocore.exceptions import ClientError
from trino.dbapi import connect
import hvac

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Vault configuration
VAULT_ADDR = os.getenv('VAULT_ADDR', 'http://vault:8200')
VAULT_TOKEN = os.getenv('VAULT_TOKEN', 'dev-root-token')
VAULT_SECRET_PATH_S3_CONN = os.getenv('VAULT_SECRET_PATH_S3_CONN')
VAULT_SECRET_PATH_TRINO_CONN = os.getenv('VAULT_SECRET_PATH_TRINO_CONN')


class VaultHandler:
    """Lightweight Vault client for reading secrets."""

    def __init__(self, vault_addr, vault_token):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self._client = None

    def get_client(self):
        """Get or create Vault client."""
        if self._client is None:
            self._client = hvac.Client(
                url=self.vault_addr,
                token=self.vault_token
            )
            if not self._client.is_authenticated():
                raise Exception("Failed to authenticate with Vault")
            logger.info(f"Authenticated with Vault at {self.vault_addr}")
        return self._client

    def get_secret(self, path):
        """
        Read a secret from Vault KV v2 engine.

        Args:
            path: Secret path (e.g., 'secret/s3/minio')

        Returns:
            Dictionary of secret data
        """
        client = self.get_client()
        try:
            secret_path_parts = path.split('/', 1)
            mount_point = secret_path_parts[0]
            secret_path = secret_path_parts[1] if len(secret_path_parts) > 1 else ''

            response = client.secrets.kv.v2.read_secret_version(
                path=secret_path,
                mount_point=mount_point
            )
            logger.info(f"Successfully read secret from {path}")
            return response['data']['data']
        except Exception as e:
            logger.error(f"Failed to read secret from {path}: {e}")
            raise


class S3Handler:
    """Lightweight S3 handler for reading files."""

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


class TrinoHandler:
    """Lightweight Trino handler for executing queries."""

    def __init__(self, host, port, user, catalog):
        self.host = host
        self.port = int(port)
        self.user = user
        self.catalog = catalog
        self._conn = None

    def get_conn(self):
        """Get or create Trino connection."""
        if self._conn is None:
            self._conn = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                http_scheme='http'
            )
            logger.info(f"Connected to Trino at {self.host}:{self.port}/{self.catalog}")
        return self._conn

    def run(self, sql):
        """
        Execute SQL query without returning results.

        Args:
            sql: SQL query string
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            logger.debug(f"Executing SQL: {sql[:200]}...")
            cursor.execute(sql)
            cursor.fetchall()  # Consume results
            logger.debug("SQL execution completed")
        finally:
            cursor.close()

    def close(self):
        """Close Trino connection."""
        if self._conn:
            self._conn.close()
            logger.info("Closed Trino connection")


def get_s3_handler():
    """Create S3 handler from Vault or environment variables."""
    logger.info("Reading S3 credentials from Vault")
    vault = VaultHandler(VAULT_ADDR, VAULT_TOKEN)
    s3_secrets = vault.get_secret(VAULT_SECRET_PATH_S3_CONN)
    return S3Handler(
        endpoint_url=s3_secrets['endpoint'],
        access_key=s3_secrets['access_key'],
        secret_key=s3_secrets['secret_key']
    )


def get_trino_handler():
    """Create Trino handler from Vault or environment variables."""
    logger.info("Reading Trino credentials from Vault")
    vault = VaultHandler(VAULT_ADDR, VAULT_TOKEN)
    trino_secrets = vault.get_secret(VAULT_SECRET_PATH_TRINO_CONN)
    return TrinoHandler(
        host=trino_secrets['host'],
        port=trino_secrets['port'],
        user=trino_secrets['user'],
        catalog=trino_secrets['catalog']
    )


def to_timestamptz_literal(ts: str) -> str:
    """
    Convert timestamp string to Trino TIMESTAMP literal.

    Args:
        ts: Timestamp string

    Returns:
        Trino TIMESTAMP literal
    """
    dt = pendulum.parse(ts).in_timezone("UTC")
    return f"TIMESTAMP '{dt.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} UTC'"


def ingest_to_iceberg(
    s3_key: str,
    source_bucket: str,
    target_table: str,
    batch_size: int
):
    """
    Ingest a single CSV file from S3 into Iceberg table via Trino.

    Args:
        s3_key: S3 key to ingest
        source_bucket: S3 bucket containing the file
        target_table: Target Iceberg table (schema.table format)
        batch_size: Number of rows per insert batch

    Returns:
        Total number of rows inserted
    """
    logger.info(f"Starting ingestion of s3://{source_bucket}/{s3_key} into {target_table}")

    # Get handlers
    s3_handler = get_s3_handler()
    trino_handler = get_trino_handler()

    total_rows = 0

    try:
        # Read CSV from S3
        content = s3_handler.read_key(key=s3_key, bucket_name=source_bucket)
        csv_buffer = StringIO(content)
        reader = csv.DictReader(csv_buffer)
        records = list(reader)

        if not records:
            logger.warning(f"No records found in {s3_key}")
            return 0

        logger.info(f"Found {len(records)} records in {s3_key}")

        # Insert records in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            values = []

            for r in batch:
                audit_event_id = r.get('audit_event_id')
                audit_operation = r.get('audit_operation')
                audit_timestamp = r.get('audit_timestamp')
                tbl_schema = r.get('tbl_schema')
                tbl_name = r.get('tbl_name')
                raw_data = r.get('raw_data', '').replace("'", "''")  # Escape single quotes

                audit_ts_literal = to_timestamptz_literal(audit_timestamp)

                values.append(
                    "("
                    "CAST(current_timestamp AS timestamp(6) with time zone),"  # ingested_at
                    f"'{s3_key}',"
                    f"{audit_event_id},"
                    f"'{audit_operation}',"
                    f"{audit_ts_literal},"
                    f"'{tbl_schema}',"
                    f"'{tbl_name}',"
                    f"'{raw_data}'"
                    ")"
                )

            sql = (
                f"INSERT INTO {target_table} "
                f"(ingested_at, source_file, audit_event_id, audit_operation, "
                f"audit_timestamp, tbl_schema, tbl_name, raw_data) "
                f"VALUES "
            ) + ",".join(values)

            logger.debug(f"Inserting batch of {len(batch)} rows")
            trino_handler.run(sql=sql)
            total_rows += len(batch)
            logger.info(f"Inserted batch={len(batch)} total={total_rows}")

        logger.info(f"Ingestion complete. Total rows inserted: {total_rows}")
        return total_rows

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise
    finally:
        trino_handler.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Ingest a CSV file from S3 into Iceberg via Trino',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--s3-key',
        required=True,
        type=str,
        help='S3 key (path) to ingest'
    )

    parser.add_argument(
        '--source-bucket',
        required=True,
        type=str,
        help='S3 bucket containing the CSV file'
    )

    parser.add_argument(
        '--target-table',
        required=True,
        type=str,
        help='Target Iceberg table in schema.table format'
    )

    parser.add_argument(
        '--batch-size',
        required=True,
        type=int,
        help='Number of rows per insert batch'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    try:
        total_rows = ingest_to_iceberg(
            s3_key=args.s3_key,
            source_bucket=args.source_bucket,
            target_table=args.target_table,
            batch_size=args.batch_size
        )

        logger.info(f"Successfully ingested {total_rows} rows")

        # Output result as JSON to stdout for XCom capture
        result = {
            'total_rows': total_rows,
            's3_key': args.s3_key
        }
        print(json.dumps(result))
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
