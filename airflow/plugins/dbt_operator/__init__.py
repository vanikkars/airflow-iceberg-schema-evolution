from airflow.plugins_manager import AirflowPlugin
from dbt_operator.opertor import DbtCoreOperator


class DbtPlugin(AirflowPlugin):
    name = "dbt_plugin"
    operators = [DbtCoreOperator]