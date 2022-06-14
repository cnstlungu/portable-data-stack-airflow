
from datetime import timedelta, datetime
import pytz
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor


AIRFLOW_CONN_SALES_DW = os.getenv('AIRFLOW_CONN_SALES_DW')
AIRFLOW_CONN_SALES_OLTP = os.getenv('AIRFLOW_CONN_SALES_OLTP')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    'import_main_data',
    default_args=default_args,
    description='Import Main Transactions Files',
    schedule_interval='@daily',
    start_date=days_ago(1),
    is_paused_upon_creation=False
)

wait_for_init = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',
    execution_date_fn = lambda x: datetime(2021, 1, 1, 0, 0, 0, 0, pytz.UTC),
    timeout=1,
    dag=dag
)

import_transactions_task = BashOperator(
    task_id='import_transactions',
    bash_command=f"""psql {AIRFLOW_CONN_SALES_OLTP} -c "\copy transactions to stdout" | psql {AIRFLOW_CONN_SALES_DW} -c "\copy import.transactions(transaction_id, customer_id, product_id, amount, qty, channel_id, bought_date)  from stdin" """,
    dag=dag,
)

import_channels_task = BashOperator(
    task_id='import_channels',
    bash_command=f"""psql {AIRFLOW_CONN_SALES_OLTP} -c "\copy channels to stdout" | psql {AIRFLOW_CONN_SALES_DW} -c "\copy import.channels(channel_id, channel_name) from stdin" """,
    dag=dag,
)

import_resellers_task = BashOperator(
    task_id='import_resellers',
    bash_command=f"""psql {AIRFLOW_CONN_SALES_OLTP} -c "\copy resellers to stdout" | psql {AIRFLOW_CONN_SALES_DW} -c "\copy import.resellers(reseller_id, reseller_name, commission_pct) from stdin" """,
    dag=dag,
)

import_customers_task = BashOperator(
    task_id='import_customers',
    bash_command=f"""psql {AIRFLOW_CONN_SALES_OLTP} -c "\copy customers to stdout" | psql {AIRFLOW_CONN_SALES_DW} -c "\copy import.customers(customer_id, first_name, last_name, email) from stdin" """,
    dag=dag,
)

import_products_task = BashOperator(
    task_id='import_products',
    bash_command=f"""psql {AIRFLOW_CONN_SALES_OLTP} -c "\copy products to stdout" | psql {AIRFLOW_CONN_SALES_DW} -c "\copy import.products(product_id, name, city, price) from stdin" """,
    dag=dag,
)

wait_for_init >> import_transactions_task >> [import_channels_task, import_customers_task, import_resellers_task, import_products_task]