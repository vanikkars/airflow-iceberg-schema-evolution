# dags/dbt_transform_ecomm.py

import pendulum
from airflow.sdk import dag
from airflow import settings
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig, ProfileConfig
from cosmos.constants import TestBehavior

# Disable Cosmos caching to avoid Pydantic serialization issues
from cosmos import settings as cosmos_settings
cosmos_settings.enable_cache = True

TRINO_CONN_ID = "trino_conn"
DBT_PROJECT_PATH = f"{settings.DAGS_FOLDER}/dbt_dwh"


@dag(
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    description="Run dbt transformations for ecommerce data using Cosmos"
)
def dbt_transform_ecomm():
    """
    DAG to run dbt transformations on ecommerce audit logs.
    Runs staging and marts models using Cosmos DbtTaskGroup.
    """

    dbt_transform = DbtTaskGroup(
        group_id='dbt_propagate_audit_logs',
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=None,
        ),
        profile_config=ProfileConfig(
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
            profile_name="dbt_dwh",
            target_name="trino_output",
        ),
        operator_args={
            "conn_id": TRINO_CONN_ID,
            "full_refresh": True,
        },
        render_config=RenderConfig(
            test_behavior=TestBehavior.NONE,
            select=['@stg_ecomm_audit_log_dml'],
        ),
    )

    return dbt_transform


dag_instance = dbt_transform_ecomm()