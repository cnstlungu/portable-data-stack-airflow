from airflow import DAG
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtRunOperator,
    DbtTestOperator
)
from airflow.utils.dates import days_ago

default_args = {
  'dir': '/usr/local/airflow/dbt',
  'start_date': days_ago(0),
  'dbt_bin': '/usr/local/airflow/.local/bin/dbt'
}

with DAG(dag_id='run_dbt_tasks', default_args=default_args, schedule_interval='@daily', ) as dag:

  dbt_seed = DbtSeedOperator(
    task_id='dbt_seed',
  )

  dbt_run = DbtRunOperator(
    task_id='dbt_run',
  )

  dbt_test = DbtTestOperator(
    task_id='dbt_test',
  )



  dbt_seed >> dbt_run >> dbt_test