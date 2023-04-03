from datetime import timedelta, datetime
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtDepsOperator
)

default_args = {
  'dir': '/usr/local/airflow/dbt',
  'start_date': days_ago(1),
  'dbt_bin': '/usr/local/airflow/.local/bin/dbt'
}

with DAG(dag_id='run_dbt_init_tasks', default_args=default_args, schedule_interval='@once', ) as dag:

  wait_for_main = ExternalTaskSensor(
    task_id='wait_for_main',
    external_dag_id='import_main_data',
    execution_date_fn = lambda x: days_ago(1),
    timeout=300,
    dag=dag
)

  wait_for_resellers = ExternalTaskSensor(
    task_id='wait_for_resellers',
    external_dag_id='import_reseller_data',
    execution_date_fn = lambda x: days_ago(1),
    timeout=300,
    dag=dag
)

  dbt_deps = DbtDepsOperator(
    task_id='dbt_deps',
  )


  dbt_seed = DbtSeedOperator(
    task_id='dbt_seed',
  )



  wait_for_main >> wait_for_resellers >> dbt_deps >> dbt_seed