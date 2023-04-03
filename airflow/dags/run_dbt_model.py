import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor


dag = DAG(
    dag_id="run_dbt_model",
    start_date=days_ago(1),
    description="A dbt wrapper for Airflow",
    schedule_interval='@daily',
)

DBT_PATH = '/usr/local/airflow/dbt'
DBT_BIN ='/usr/local/airflow/.local/bin/dbt'


wait_for_dbt_init = ExternalTaskSensor(
    task_id='wait_for_dbt_init',
    external_dag_id='run_dbt_init_tasks',
    execution_date_fn = lambda x: days_ago(1),
    timeout=300,
    dag=dag
)

# Adapted from https://docs.astronomer.io/learn/airflow-dbt

def load_manifest():
    local_filepath = f"{DBT_PATH}/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)

    return data


def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = DBT_PATH
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            cd {DBT_DIR} &&
            {DBT_BIN} {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            dag=dag,
        )

    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd {DBT_DIR} &&
            {DBT_BIN} {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            dag=dag,
        )

    return dbt_task


data = load_manifest()

dbt_tasks = {}
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")

        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]



wait_for_dbt_init >> tuple(dbt_tasks.values())