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
from airflow.timetables.interval import DeltaDataIntervalTimetable
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow import settings
from docker.types import Mount

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



S3_BUCKET_NAME = "lake"
S3_PREFIX = "raw/ecommerce/orders"


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

    extract_docker = DockerOperator(
        task_id='extract_audit_logs',
        image='audit-log-extractor:latest',
        api_version='auto',
        auto_remove='success',
        command=[
            '--data-interval-start', '{{ data_interval_start }}',
            '--data-interval-end', '{{ data_interval_end }}'
        ],
        environment={
            'PG_HOST': 'ecommerce-db',
            'PG_PORT': '5432',
            'PG_DATABASE': 'ecom',
            'PG_USER': 'ecom',
            'PG_PASSWORD': 'ecom',
            'S3_ENDPOINT': 'http://minio:9000',
            'S3_ACCESS_KEY': 'admin',
            'S3_SECRET_KEY': 'password',
            'S3_BUCKET_NAME': S3_BUCKET_NAME,
            'S3_PREFIX': S3_PREFIX,
            'EXTRACT_BATCH_SIZE': str(EXTRACT_BATCH_SIZE)
        },
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow-iceberg-schema-evolution_default',
        mounts=[
            Mount(source='/tmp', target='/tmp', type='bind')
        ],
        mount_tmp_dir=False,
        retries=3,
        retry_delay=pendulum.duration(seconds=30)
    )

    @task()
    def read_extracted_files() -> list[str]:
        """Read the list of extracted files from the Docker container output."""
        import logging
        logger = logging.getLogger("airflow.task")

        try:
            with open('/tmp/extracted_files.txt', 'r') as f:
                files = [line.strip() for line in f if line.strip()]
            logger.info(f"Read {len(files)} extracted file paths")
            return files
        except FileNotFoundError:
            logger.warning("No extracted files found, returning empty list")
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


    @task(retries=3, retry_delay=pendulum.duration(seconds=10))
    def load_raw_jsons_to_iceberg(s3_key: str):
        from io import StringIO

        logger = logging.getLogger("airflow.task")
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        content = s3.read_key(key=s3_key, bucket_name=S3_BUCKET_NAME)
        csv_buffer = StringIO(content)
        reader = csv.DictReader(csv_buffer)
        records = list(reader)
        if not records:
            logger.info("No records to load.")

        trino = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        batch_size = INGEST_BATCH_SIZE
        total = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            values = []
            for r in batch:
                audit_event_id = r.get('audit_event_id')
                audit_operation = r.get('audit_operation')
                audit_timestamp = r.get('audit_timestamp')
                tbl_schema = r.get('tbl_schema')
                tbl_name = r.get('tbl_name')
                raw_data = r.get('raw_data')

                audit_ts_literal = to_timestamptz_literal(audit_timestamp)

                logger.info(
                    f"trying to insert the row: "
                    f"{audit_event_id}, {audit_operation}, {audit_timestamp}, {tbl_schema}, {tbl_name}, {raw_data}"
                )
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
                f"INSERT INTO {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} "
                f"(ingested_at, source_file, audit_event_id, audit_operation, audit_timestamp, tbl_schema, tbl_name, raw_data) "
                f"VALUES "
            ) + ",".join(values)
            logger.info(f'Running the insert SQL: {sql}')
            trino.run(sql=sql)
            total += len(batch)
            logger.info(f"Inserted batch={len(batch)} total={total}")
        logger.info(f"Finished loading {total} rows into {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE}")
        return total


    dbt_transform = DbtCoreOperator(
        task_id='dbt_propagate_audit_logs',
        dbt_project_dir=DBT_PROJECT_PATH,
        dbt_profiles_dir=DBT_PROJECT_PATH,
        dbt_command='run',
        select='@stg_ecomm_audit_log_dml',
        full_refresh=True
    )

    # Task dependencies
    raw_s3_keys = read_extracted_files()

    create_iceberg_raw_json_tbl >> extract_docker >> raw_s3_keys >> load_raw_jsons_to_iceberg.expand(s3_key=raw_s3_keys) >> dbt_transform


dag_instance = audit_log_extract_with_data_intervals_dag()