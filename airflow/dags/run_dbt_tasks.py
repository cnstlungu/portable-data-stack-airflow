from airflow import DAG

from datetime import datetime
from airflow.utils import timezone
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtRunOperator,
    DbtTestOperator
)

now = timezone.utcnow()

default_args = {
  'dir': '/usr/local/airflow/dbt',
  'start_date': timezone.datetime(2021,1,1),
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