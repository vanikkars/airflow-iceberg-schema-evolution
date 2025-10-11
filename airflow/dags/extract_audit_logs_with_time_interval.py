# dags/audit_log_extract_dag.py
from __future__ import annotations
import os
import csv
import pendulum
import logging
from airflow.sdk import dag, task, get_current_context
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum as pnd
from airflow.timetables.interval import DeltaDataIntervalTimetable

S3_CONN_ID = "s3_conn"
PG_CONN_ID = "ecom_audit_logs"          # Define this connection in Airflow UI

DAG_ID = "audit_log_extract"
LAST_TS_VAR = f"{DAG_ID}_last_audit_timestamp"
OUTPUT_DIR = "/opt/airflow/data/extracts"
BATCH_SIZE = 3

S3_BUCKET_NAME = "lake"
S3_PREFIX = "raw/ecommerce/orders"


@dag(
    start_date=pnd.datetime(2024, 1, 1, tz="UTC"),
    schedule=DeltaDataIntervalTimetable(delta=pendulum.duration(days=1)),
    catchup=False,
    default_args={"owner": "airflow", "retries": 0},
    description="Incremental extract from audit_log_dml based on audit_timestamp."
)
def audit_log_extract_with_data_intervals_dag():

    @task()
    def extract(
            logical_date: pnd.DateTime = None,
    ) -> list[str]:
        from io import StringIO

        logger = logging.getLogger("airflow.task")
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        sql = """
            SELECT event_id,
                   audit_operation,
                   audit_timestamp,
                   tbl_schema,
                   tbl_name,
                   raw_data
            FROM audit_log_dml
            WHERE audit_timestamp >= '{}' AND audit_timestamp < '{}'
            ORDER BY audit_timestamp
            LIMIT {} OFFSET {}
        """
        start_date = logical_date.date()
        # start_date = logical_date.replace(minute=0, second=0, microsecond=0)
        # we need to set to zero hours, so it always pulls everything from the midnight
        end_date = start_date + pnd.duration(days=1)
        # logger.info(f"{data_interval_start=}, {data_interval_end=}, {logical_date=}")

        logger.info(f"start_date: {start_date}, end_date: {end_date}")

        extracted_files = []
        offset = 0
        iteration = 0
        batch_size = BATCH_SIZE
        while True:
            iteration += 1
            formatted_sql = sql.format(start_date, end_date, batch_size, offset)
            # logger.info(f'run the query: {formatted_sql}')
            logger.info('='*200)
            logger.info(f'iteration: {iteration}')
            rows = hook.get_records(formatted_sql)
            if not rows:
                logger.info('No more rows to fetch, exiting loop.')
                break

            if rows:
                s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

                header = ["event_id", "audit_operation", "audit_timestamp", "tbl_schema", "tbl_name", "raw_data"]
                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(header)
                csv_writer.writerows(rows)

                s3_key = (
                    f"{S3_PREFIX}/audit_log_{start_date.isoformat()}_{end_date.isoformat()}_{batch_size}_{offset}.csv"
                ).replace(":", "-")

                s3_hook.load_string(
                    csv_buffer.getvalue(),
                    key=s3_key,
                    bucket_name=S3_BUCKET_NAME,
                    replace=True
                )

                extracted_files.append(s3_key)
                logger.info(
                    f"{iteration=} Wrote {len(rows)} records to s3://{S3_BUCKET_NAME}/{s3_key}, {batch_size=}, {offset=}"
                )

            offset += batch_size
        return extracted_files
            # TODO check where we last stopped, instead of always starting from the beginning in this time window
                # max_ts = max(r[2] for r in rows)
                # Variable.set(LAST_TS_VAR, max_ts.isoformat() if hasattr(max_ts, "isoformat") else str(max_ts))
            # else:
            #     # No data; advance marker to end_dt to avoid re-scanning
            #     Variable.set(LAST_TS_VAR, end_date.isoformat())


        create_iceberg_raw_json = SQLExecuteQueryOperator(
            task_id="create_iceberg_rides_raw_json",
            conn_id=TRINO_CONN_ID,
            sql=f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} (
                ingested_at   timestamp,
                source_file  varchar,
                raw_data     varchar
            )
            WITH (
                format='PARQUET',
                partitioning=ARRAY['day(ingested_at)']
            )
            """
        )

    extract()


dag_instance = audit_log_extract_with_data_intervals_dag()