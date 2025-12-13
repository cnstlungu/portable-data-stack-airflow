from datetime import timedelta
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

# Constants
DBT_PATH = '/datamart/postcard_company'
DBT_BIN = '/home/airflow/.local/bin/dbt'

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='run_dbt',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='@once',
    catchup=False,
    tags=['dbt', 'transformation', 'data-warehouse'],
    doc_md="""
    ### dbt Full Pipeline
    
    Runs the complete dbt workflow in sequence:
    1. **deps**: Install dbt package dependencies
    2. **seed**: Load static reference data (e.g., geography)
    3. **compile**: Compile dbt models to SQL
    4. **run**: Execute all transformations
    5. **test**: Run data quality tests
    
    This DAG should be run once after initial setup or when dbt project changes.
    """
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PATH} && {DBT_BIN} deps'
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PATH} && {DBT_BIN} seed'
    )

    dbt_compile = BashOperator(
        task_id='dbt_compile',
        bash_command=f'cd {DBT_PATH} && {DBT_BIN} compile'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PATH} && {DBT_BIN} run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PATH} && {DBT_BIN} test'
    )

    # Task dependencies
    dbt_deps >> dbt_seed >> dbt_compile >> dbt_run >> dbt_test