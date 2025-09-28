from airflow.plugins_manager import AirflowPlugin
from dbt_operator.operator import DbtCoreOperator


class DbtPlugin(AirflowPlugin):
    name = "dbt_plugin"
    operators = [DbtCoreOperator]