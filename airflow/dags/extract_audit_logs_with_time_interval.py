# dags/audit_log_extract_dag.py

import csv
import pendulum
import logging
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum as pnd
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow import settings

from dbt_operator import DbtCoreOperator

S3_CONN_ID = "s3_conn"
PG_CONN_ID = "ecom_audit_logs"
TRINO_CONN_ID = "trino_conn"

DAG_ID = "audit_log_extract"
LAST_TS_VAR = f"{DAG_ID}_last_audit_timestamp"
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
VAULT_SECRET_PATH_TRINO_CONN = "secret/trino/iceberg"

S3_BUCKET_NAME = "lake"
S3_PREFIX = "raw/ecommerce/audit_log_dml"


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
def audit_log_extract_with_data_intervals_dag():

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
        retries=3,
        retry_delay=pendulum.duration(seconds=30)
    )


    @task()
    def parse_extraction_result(**context) -> list[str]:
        """Parse the JSON output from the Docker extractor and return the list of S3 keys."""
        import logging
        import json

        logger = logging.getLogger("airflow.task")

        # Get the XCom value from the extract_docker task
        ti = context['ti']
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

    # drop_table = SQLExecuteQueryOperator(
    #     task_id="drop_table",
    #     conn_id=TRINO_CONN_ID,
    #     sql=f"""
    #     DROP TABLE IF EXISTS {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE}
    #     """
    # )

    create_iceberg_raw_json_tbl = SQLExecuteQueryOperator(
        task_id="create_iceberg_raw_json",
        conn_id=TRINO_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} (
            ingested_at      timestamp(6) with time zone,
            source_file      varchar,
            audit_event_id   bigint,
            audit_operation  varchar,
            audit_timestamp  timestamp(6) with time zone,
            tbl_schema       varchar,
            tbl_name         varchar,
            raw_data         varchar
        )
        WITH (
            format='PARQUET',
            partitioning=ARRAY['day(ingested_at)']
        )
        """
    )


    @task()
    def build_ingest_commands(s3_keys: list[str]) -> list[list[str]]:
        """Build command lists for each S3 key to ingest."""
        commands = []
        for s3_key in s3_keys:
            commands.append([
                '--s3-key', s3_key,
                '--source-bucket', S3_BUCKET_NAME,
                '--target-table', f'{ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE}',
                '--batch-size', str(INGEST_BATCH_SIZE),
            ])
        return commands


    dbt_transform = DbtCoreOperator(
        task_id='dbt_propagate_audit_logs',
        dbt_project_dir=DBT_PROJECT_PATH,
        dbt_profiles_dir=DBT_PROJECT_PATH,
        dbt_command='run',
        select='@stg_ecomm_audit_log_dml',
        full_refresh=True
    )


    # Task dependencies
    raw_s3_keys = parse_extraction_result()
    ingest_commands = build_ingest_commands(raw_s3_keys)

    load_to_iceberg = DockerOperator.partial(
        task_id='load_to_iceberg',
        image='iceberg-ingestor:latest',
        api_version='auto',
        auto_remove='success',
        environment={
            'VAULT_ADDR': VAULT_ADDR,
            'VAULT_TOKEN': VAULT_TOKEN,
            'VAULT_SECRET_PATH_S3_CONN': VAULT_SECRET_PATH_S3_CONN_TARGET_BUCKET,
            'VAULT_SECRET_PATH_TRINO_CONN': VAULT_SECRET_PATH_TRINO_CONN,
        },
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow-iceberg-schema-evolution_default',
        mount_tmp_dir=False,
        retries=3,
        retry_delay=pendulum.duration(seconds=10)
    ).expand(command=ingest_commands)

    create_iceberg_raw_json_tbl >> extract_audit_logs >> raw_s3_keys >> ingest_commands >> load_to_iceberg >> dbt_transform


dag_instance = audit_log_extract_with_data_intervals_dag()