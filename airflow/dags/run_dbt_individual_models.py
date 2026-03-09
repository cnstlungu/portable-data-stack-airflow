import json
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

# Constants
DBT_PATH = '/datamart/postcard_company'
DBT_BIN = '/home/airflow/.local/bin/dbt'

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    dag_id="run_dbt_individual_models",
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    description="A dbt wrapper for Airflow",
    schedule='@once',
    catchup=False,
    max_active_tasks=1,  # Ensure serial execution to avoid DuckDB lock conflicts
    tags=['dbt', 'transformation', 'individual-models'],
    doc_md="""
    ### dbt Individual Model Pipeline
    
    Runs dbt models individually based on the compiled manifest.json file.
    Each model is executed as a separate task with its own test task.
    
    **Workflow:**
    1. **deps**: Install dbt dependencies
    2. **seed**: Load static reference data
    3. **Individual models**: Run each model separately (respects dependencies)
    4. **Individual tests**: Test each model after it runs
    
    **Note:** Tasks run serially (max_active_tasks=1) to prevent DuckDB file locking issues.
    """
)

# Adapted from https://docs.astronomer.io/learn/airflow-dbt

def load_manifest():
    """Load dbt manifest.json, returning empty structure if not found."""
    local_filepath = f"{DBT_PATH}/target/manifest.json"
    try:
        with open(local_filepath) as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"nodes": {}}

    return data


def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator to run or test an individual model."""
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            cd {DBT_PATH} &&
            {DBT_BIN} {GLOBAL_CLI_FLAGS} {dbt_verb} --target local --models {model}
            """,
            dag=dag,
        )

    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd {DBT_PATH} &&
            {DBT_BIN} {GLOBAL_CLI_FLAGS} {dbt_verb} --target local --models {model}
            """,
            dag=dag,
        )

    return dbt_task


# Load manifest and create tasks
data = load_manifest()

dbt_tasks = {}
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")

        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

# Set up task dependencies
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        # Set dependency to run tests on a model after model run finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]

# Setup tasks
dbt_deps = BashOperator(
    task_id='dbt_deps',
    dag=dag,
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} deps'
)

dbt_seed = BashOperator(
    task_id='dbt_seed',
    dag=dag,
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} seed'
)

# Task dependencies
dbt_deps >> dbt_seed >> tuple(dbt_tasks.values())