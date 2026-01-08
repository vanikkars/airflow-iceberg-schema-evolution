# dags/audit_log_extract_dag.py

import pendulum
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum as pnd
from airflow import settings

from dbt_operator import DbtCoreOperator

S3_CONN_ID = "s3_conn"
PG_CONN_ID = "ecom_audit_logs"
TRINO_CONN_ID = "trino_conn"

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
def extract_audit_logs_ecomm():

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

    create_iceberg_raw_json_tbl = SQLExecuteQueryOperator(
        task_id="create_iceberg_raw_json",
        conn_id=TRINO_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS iceberg.{ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} (
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
    def build_insert_queries(s3_keys: list[str]) -> list[str]:
        """Build Trino INSERT queries for each S3 key using Hive CSV external table."""
        queries = []
        for idx, s3_key in enumerate(s3_keys):
            s3_path = f's3a://{S3_BUCKET_NAME}/{s3_key}'
            # Create unique temp table name for each file
            temp_table = f"temp_csv_load_{ICEBERG_RAW_JSON_TABLE}_{pendulum.now('UTC').strftime('%Y_%m_%d_%H_%M_%S')}_{abs(hash(s3_key))}"

            # Use Hive catalog for temp CSV tables, then insert into Iceberg
            query = f"""
            DROP TABLE IF EXISTS hive.default.{temp_table};
            CREATE TABLE hive.default.{temp_table} (
                audit_event_id varchar,
                audit_operation varchar,
                audit_timestamp varchar,
                tbl_schema varchar,
                tbl_name varchar,
                raw_data varchar
            )
            WITH (
                external_location = '{s3_path}',
                format = 'CSV',
                csv_separator = ',',
                skip_header_line_count = 1
            );
            INSERT INTO iceberg.{ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE}
            SELECT
                current_timestamp AT TIME ZONE 'UTC' AS ingested_at,
                '{s3_path}' AS source_file,
                CAST(audit_event_id AS bigint),
                audit_operation,
                CAST(audit_timestamp AS timestamp(6) with time zone),
                tbl_schema,
                tbl_name,
                raw_data
            FROM hive.default.{temp_table};
            DROP TABLE hive.default.{temp_table};
            """
            queries.append(query)
        return queries

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
    insert_queries = build_insert_queries(raw_s3_keys)

    load_to_iceberg = SQLExecuteQueryOperator.partial(
        task_id='load_to_iceberg',
        conn_id=TRINO_CONN_ID,
        split_statements=True
    ).expand(sql=insert_queries)

    create_iceberg_raw_json_tbl >> extract_audit_logs >> raw_s3_keys >> insert_queries >> load_to_iceberg >> dbt_transform


dag_instance = extract_audit_logs_ecomm()