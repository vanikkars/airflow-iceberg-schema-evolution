import json
import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import requests
from airflow.sdk import dag, task, get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dbt_operator import DbtCoreOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from pydantic import BaseModel
from airflow import settings

# Configuration
API_URL = "http://ride_api:8000/rides"
S3_BUCKET_NAME = "lake"
S3_PREFIX = "raw/uber/rides"

DWH_INGESTION_SCHEMA = "ingestion"
UBER_RIDES_TABLE = "uber_rides"

ICEBERG_DB = "landing"
ICEBERG_RAW_JSON_TABLE = "rides_raw"          # 3-column raw capture
DBT_PROJECT_PATH = f"{settings.DAGS_FOLDER}/dbt_dwh"


S3_CONN_ID = "s3_conn"
DWH_CONN_ID = "dwh_conn"
TRINO_CONN_ID = "trino_conn"

def render_sql(template_name: str, **kwargs) -> str:
    logger = logging.getLogger("airflow.task")
    from jinja2 import Environment, FileSystemLoader
    TEMPLATES_DIR = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    template = env.get_template(template_name).render(**kwargs)
    logger.info(f"rendered_template: {template}")
    return template

class RideBookingInput(BaseModel):
    booking_date: dt.date
    booking_time: Optional[dt.time]
    booking_id: str
    booking_status: str
    customer_id: str
    vehicle_type: str
    pickup_location: str
    drop_location: str
    avg_vtat: Optional[float] = None
    avg_ctat: Optional[float] = None
    cancelled_rides_by_customer: Optional[int] = None
    reason_for_cancelling_by_customer: Optional[str] = None
    cancelled_rides_by_driver: Optional[int] = None
    driver_cancellation_reason: Optional[str] = None
    incomplete_rides: Optional[int] = None
    incomplete_rides_reason: Optional[str] = None
    booking_value: Optional[float] = None
    ride_distance: Optional[float] = None
    driver_ratings: Optional[float] = None
    customer_rating: Optional[float] = None
    payment_method: Optional[str] = None

    @classmethod
    def get_columns(cls) -> list[str]:
        return list(cls.model_fields.keys())

class RideBookingExtended(RideBookingInput):
    source_file: Optional[str] = None

@dag(
    start_date=dt.datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False
)
def uber_rides_iceberg():

    @task
    def uber_rides_raw():
        logger = logging.getLogger("airflow.task")
        context = get_current_context()
        execution_date = context["logical_date"].strftime("%Y-%m-%d")
        try:
            resp = requests.get(API_URL, params={"start_date": execution_date, "end_date": execution_date}, timeout=30)
            resp.raise_for_status()
            rides = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API error: {e}")
            raise
        s3_key = f"{S3_PREFIX}/{execution_date}/rides_{execution_date}.json"
        S3Hook(aws_conn_id=S3_CONN_ID).load_string(
            string_data=json.dumps(rides),
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logger.info(f"Wrote {len(rides)} records to s3://{S3_BUCKET_NAME}/{s3_key}")
        return s3_key

    create_iceberg_raw_json = SQLExecuteQueryOperator(
        task_id="create_iceberg_rides_raw_json",
        conn_id=TRINO_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} (
            created_at   timestamp,
            source_file  varchar,
            raw_data     varchar
        )
        WITH (
            format='PARQUET',
            partitioning=ARRAY['day(created_at)']
        )
        """
    )

    @task
    def load_raw_jsons_to_iceberg(s3_key: str):
        logger = logging.getLogger("airflow.task")
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        content = s3.read_key(key=s3_key, bucket_name=S3_BUCKET_NAME)
        records = json.loads(content)
        if not records:
            logger.info("No records to load.")
            return 0
        trino = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        batch_size = 500
        total = 0

        def esc(s: str) -> str:
            return s.replace("'", "''")

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            values = []
            for r in batch:
                raw_json = json.dumps(r, separators=(",", ":"), ensure_ascii=False)
                values.append(f"(current_timestamp, '{esc(s3_key)}', '{esc(raw_json)}')")
            sql = f"INSERT INTO {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE} (created_at, source_file, raw_data) VALUES " + ",".join(values)
            trino.run(sql=sql)
            total += len(batch)
            logger.info(f"Inserted batch={len(batch)} total={total}")
        logger.info(f"Finished loading {total} rows into {ICEBERG_DB}.{ICEBERG_RAW_JSON_TABLE}")
        return total

    dbt_transform = DbtCoreOperator(
        task_id='dbt_transform_rides',
        dbt_project_dir=DBT_PROJECT_PATH,
        dbt_profiles_dir=DBT_PROJECT_PATH,
        dbt_command='run',
        select='@stg_uber_rides',
        # dbt_command='run --select @stg_uber_rides',
        full_refresh=True
    )

    raw_key = uber_rides_raw()

    create_iceberg_raw_json >> load_raw_jsons_to_iceberg(raw_key) >> dbt_transform

uber_rides_iceberg()