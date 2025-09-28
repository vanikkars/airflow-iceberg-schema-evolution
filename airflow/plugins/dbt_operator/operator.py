from __future__ import annotations
import json
from typing import Any, Dict, Optional, Sequence
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from dbt.cli.main import dbtRunner, dbtRunnerResult


class DbtCoreOperator(BaseOperator):
    template_fields: Sequence[str] = ("select", "target", "dbt_vars")

    def __init__(
        self,
        dbt_project_dir: str,
        dbt_profiles_dir: str,
        dbt_command: str,
        target: Optional[str] = None,
        select: Optional[str] = None,
        dbt_vars: Optional[Dict[str, Any]] = None,
        full_refresh: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_command = dbt_command
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.target = target
        self.select = select
        self.runner = dbtRunner()
        self.dbt_vars = dbt_vars or {}
        self.full_refresh = full_refresh

    def execute(self, context):
        command_args = [
            self.dbt_command,
            "--profiles-dir", self.dbt_profiles_dir,
            "--project-dir", self.dbt_project_dir,
        ]
        if self.target:
            command_args += ["--target", self.target]
        if self.select:
            command_args += ["--select", self.select]
        if self.full_refresh:
            command_args.append("--full-refresh")
        if self.dbt_vars:
            command_args += ["--vars", json.dumps(self.dbt_vars)]

        self.log.info("Executing dbt: %s", " ".join(command_args))
        res: dbtRunnerResult = self.runner.invoke(command_args)

        if res.exception:
            raise AirflowException(f"dbt failed: {res.exception}")

        for r in getattr(res, "result", []):
            self.log.info("%s: %s", r.node.name, r.status)

        return {r.node.name: r.status for r in getattr(res, "result", [])}