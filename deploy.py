from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List

import yaml
from snowflake.core import CreateMode, Root  # type: ignore[reportPrivateImportUsage]
from snowflake.core.task import Cron
from snowflake.core.task.dagv1 import DAG, DAGOperation, DAGTask
from snowflake.snowpark import Session

# Load YAML configuration


class DAGActionType(Enum):
    CREATE_OR_REPLACE = "createOrReplace"
    DROP = "drop"

    @staticmethod
    def values():
        return [action.value for action in DAGActionType]


class SnowflakeDAGs:
    """Provides a context manager and methods to deploy DAGs to Snowflake"""

    def __init__(self, connection_name: str, config_file_path: Path):
        self._connection_name: str = connection_name
        self._config_file_path: Path = config_file_path

        self._dags: List[Dict[DAGActionType, DAG]] = []

    def __enter__(self):
        self._session = Session.builder.config(
            "connection_name", self.connection_name
        ).create()
        self.root = Root(self._session)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    @property
    def config_file_path(self):
        return self._config_file_path

    @config_file_path.setter
    def config_file_path(self, config_file_path: Path):
        self._config_file_path = config_file_path
        del self.dag_bag

    @property
    def connection_name(self):
        return self._connection_name

    @property
    def dag_bag(self):
        return self._dags

    @dag_bag.deleter
    def dag_bag(self):
        self._dags = []

    @property
    def dag_config(self):
        return self.load_config(self.config_file_path)

    @property
    def database(self) -> str:
        assert self.root.connection.database, "Database not set in connection"
        return self.root.connection.database

    @property
    def schema(self) -> str:
        assert self.root.connection.schema, "Schema not set in connection"
        return self.root.connection.schema

    def bag_dags(self):
        """Bag the DAGs to be deployed"""
        del self.dag_bag
        for dag_config in self.dag_config.get("dags", []):
            name = dag_config["name"]
            if not dag_config.get("disable", False):
                schedule = self._get_schedule(dag_config)

                dag = DAG(
                    name=name,
                    schedule=schedule,
                    **dag_config.get("options", {}),
                )
                task_dict = self._create_dag_tasks(dag_config, dag)
                self._add_task_dependencies(dag_config, task_dict)
                self._bag_dag(dag_config, dag)

            message = f"Bagged DAG: {name}" if not dag_config.get("disable", False) else f"Skipped DAG: {name}"

            print(message)
            

    def deploy_dags(self):
        """Deploy the bagged DAGs"""
        if not self.dag_bag:
            self.bag_dags()

        for dag_dict in self.dag_bag:
            action, dag = list(dag_dict.items())[0]
            schema = self.root.databases[self.database].schemas[self.schema]
            dag_op = DAGOperation(schema)
            if action == DAGActionType.CREATE_OR_REPLACE:
                dag_op.deploy(dag, mode=CreateMode.or_replace)
            elif action == DAGActionType.DROP:
                dag_op.drop(dag)
                self._drop_finalizer_task(dag, schema)
            print(f"{action.value.capitalize()} DAG: {dag.name}")

    def _drop_finalizer_task(self, dag, schema):
        # finalizier task becomes the root after DAG is dropped and must be dropped separately
        finalizer_task = dag.get_finalizer_task()
        if finalizer_task:
            finalizer_dag = DAG(name=finalizer_task.full_name)
            DAGOperation(schema).drop(finalizer_dag)

    def load_config(self, file_path: Path) -> dict:
        try:
            with open(file_path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error loading configuration file {file_path}: {e}")
            return {}


    def _get_schedule(self, dag_config):
        schedule = dag_config.get("schedule")

        if any(el for el in ["MINUTE", "MINUTES"] if el in schedule.upper()):
            schedule = timedelta(minutes=int(schedule.split(" ")[0]))
        elif "DAYS" in schedule.upper():
            schedule = timedelta(days=int(schedule.split(" ")[0]))
        else:
            if schedule:
                schedule = Cron(schedule, dag_config.get("timezone", 'America/Los_Angeles'))
        
        return schedule


    def _create_dag_tasks(self, dag_config, dag):
        task_dict = {}
        for task in dag_config.get("tasks", []):
            dag_task = DAGTask(
                    name=task["name"],
                    definition=task["sql"],
                    warehouse=task.get("warehouse"),
                    session_parameters=task.get("session_parameters"),
                    user_task_managed_initial_warehouse_size=task.get(
                        "user_task_managed_initial_warehouse_size"
                    ),
                    user_task_timeout_ms=task.get("user_task_timeout_ms"),
                    error_integration=task.get("error_integration"),
                    comment=task.get("comment"),
                    is_finalizer=task.get("is_finalizer", False),
                    dag=dag,
                )
            task_dict[task["name"]] = dag_task
            dag.add_task(dag_task)
        return task_dict

    def _add_task_dependencies(self, dag_config, task_dict):
        for task in dag_config.get("tasks", []):
            dag_task = task_dict[task["name"]]
            if task.get("predecessors"):
                for predecessor in task["predecessors"]:
                    dag_task.add_predecessors(task_dict[predecessor])

            if task.get("successors"):
                dag_task.add_successors(task_dict[task["successors"]])

    def _bag_dag(self, dag_config, dag):
        action: DAGActionType = DAGActionType(
                dag_config.get("action", DAGActionType.CREATE_OR_REPLACE.value)
            )
        self._dags.append({action: dag})


# Main function
def main():
    with SnowflakeDAGs("myconnection", Path("dags.yml")) as api:
        api.bag_dags()
        api.deploy_dags()


if __name__ == "__main__":
    main()
