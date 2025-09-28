from airflow.plugins_manager import AirflowPlugin
from dbt_operator.dbt_core_operator import DbtCoreOperator


class DbtPlugin(AirflowPlugin):
    name = "dbt_plugin"
    operators = [DbtCoreOperator]