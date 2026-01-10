# dags/audit_log_extract_dag.py

import sys
import os
import pendulum
from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum as pnd
from airflow import settings
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig, ProfileConfig
from cosmos.constants import TestBehavior

# Add plugins to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from hooks.duckdb_hook import DuckDBHook

S3_CONN_ID = "s3_conn"
PG_CONN_ID = "ecom_audit_logs"
DUCKDB_PATH = "/opt/airflow/data/iceberg.duckdb"

OUTPUT_DIR = "/opt/airflow/data/extracts"
EXTRACT_BATCH_SIZE = 7
INGEST_BATCH_SIZE = 7

ICEBERG_DB = "landing"
ICEBERG_RAW_JSON_TABLE = "ecomm_audit_log_dml"          # 3-column raw capture
DBT_PROJECT_PATH = f"{settings.DAGS_FOLDER}/dbt_dwh"

VAULT_ADDR = "http://vault:8200"
VAULT_TOKEN = "dev-root-token"
VAULT_SECRET_PATH_S3_CONN_TARGET_BUCKET = "secret/s3/minio"
VAULT_SECRET_PATH_PSQL_SOURCE_DB = "secret/postgres/ecommerce"

S3_BUCKET_NAME = "lake"
S3_PREFIX = "raw/ecommerce/audit_log_dml"
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
S3_REGION = "us-east-1"


def to_timestamptz_literal(ts: str) -> str:
    # Parse, normalize to UTC, format microseconds
    dt = pendulum.parse(ts).in_timezone("UTC")
    return f"TIMESTAMP '{dt.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} UTC'"


@dag(
    start_date=pnd.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    default_args={"owner": "airflow", "retries": 0},
    description="Incremental extract from audit_log_dml based on audit_timestamp."
)
def extract_audit_logs_ecomm_full():

    extract_audit_logs = DockerOperator(
        task_id='extract_audit_logs',
        image='audit-log-extractor:latest',
        api_version='auto',
        auto_remove='success',
        command=[
            '--data-interval-start', '{{ data_interval_start }}',
            '--data-interval-end', '{{ data_interval_end }}',
            '--batch-size', str(EXTRACT_BATCH_SIZE),
            '--target-bucket', S3_BUCKET_NAME,
            '--target-prefix', S3_PREFIX,
        ],
        environment={
            'VAULT_ADDR': VAULT_ADDR,
            'VAULT_TOKEN': VAULT_TOKEN,
            'VAULT_SECRET_PATH_S3_CONN_TARGET_BUCKET': VAULT_SECRET_PATH_S3_CONN_TARGET_BUCKET,
            'VAULT_SECRET_PATH_PSQL_SOURCE_DB': VAULT_SECRET_PATH_PSQL_SOURCE_DB,
        },
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow-iceberg-schema-evolution_default',
        mount_tmp_dir=False,
        retries=0,
        retry_delay=pendulum.duration(seconds=30)
    )

    @task()
    def parse_extraction_result(ti) -> list[str]:
        """Parse the JSON output from the Docker extractor and return the list of S3 keys."""
        import logging
        import json

        logger = logging.getLogger("airflow.task")
        docker_output = ti.xcom_pull(task_ids='extract_audit_logs')
        logger.info(f"Raw Docker output: {docker_output}")
        if not docker_output:
            logger.warning("No output from Docker extraction, returning empty list")
            return []
        try:
            # Parse JSON output
            result = json.loads(docker_output)
            extracted_files = result.get('extracted_files', [])
            logger.info(f"Parsed {len(extracted_files)} extracted file paths")
            return extracted_files
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Docker output as JSON: {e}")
            return []

    @task()
    def create_iceberg_raw_json_tbl():
        """Create Iceberg landing table with DuckDB."""
        hook = DuckDBHook(
            db_path=DUCKDB_PATH,
            s3_endpoint=S3_ENDPOINT,
            s3_access_key=S3_ACCESS_KEY,
            s3_secret_key=S3_SECRET_KEY,
            s3_region=S3_REGION
        )

        with hook:
            # Create schema if it doesn't exist
            hook.create_schema(ICEBERG_DB)

            sql = f"""
            CREATE TABLE IF NOT EXISTS iceberg.{ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} (
                ingested_at      TIMESTAMP WITH TIME ZONE,
                source_file      VARCHAR,
                audit_event_id   BIGINT,
                audit_operation  VARCHAR,
                audit_timestamp  TIMESTAMP WITH TIME ZONE,
                tbl_schema       VARCHAR,
                tbl_name         VARCHAR,
                raw_data         VARCHAR
            )
            """

            hook.run(sql)

    @task()
    def load_csv_to_iceberg(s3_keys: list[str]):
        """Load CSV data from S3 into Iceberg landing table."""
        hook = DuckDBHook(
            db_path=DUCKDB_PATH,
            s3_endpoint=S3_ENDPOINT,
            s3_access_key=S3_ACCESS_KEY,
            s3_secret_key=S3_SECRET_KEY,
            s3_region=S3_REGION
        )

        with hook:
            for s3_key in s3_keys:
                # S3 path using s3a:// protocol for MinIO compatibility with DuckDB
                s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_key}"

                # Load CSV and add ingested_at and source_file columns
                load_sql = f"""
                INSERT INTO iceberg.{ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE}
                SELECT
                    NOW() as ingested_at,
                    '{s3_key}' as source_file,
                    audit_event_id,
                    audit_operation,
                    audit_timestamp,
                    tbl_schema,
                    tbl_name,
                    raw_data
                FROM read_csv(
                    '{s3_path}',
                    header=true,
                    delim=','
                );
                """

                hook.run(load_sql)

    dbt_transform = DbtTaskGroup(
        group_id='dbt_propagate_audit_logs',
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=None,
        ),
        profile_config=ProfileConfig(
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
            profile_name="dbt_dwh",
            target_name="duckdb_iceberg",
        ),
        operator_args={
            "full_refresh": False,
        },
        render_config=RenderConfig(
            test_behavior=TestBehavior.NONE,
            select=['staging'],
        ),
    )

    # Task dependencies
    create_table = create_iceberg_raw_json_tbl()
    raw_s3_keys = parse_extraction_result()
    load_data = load_csv_to_iceberg(raw_s3_keys)

    create_table >> extract_audit_logs >> raw_s3_keys >> load_data >> dbt_transform


dag_instance = extract_audit_logs_ecomm_full()