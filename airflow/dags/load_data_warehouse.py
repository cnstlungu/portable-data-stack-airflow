
from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DagRun

def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    print(dag_runs)
    return dag_runs[0] if dag_runs else None


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
    'load_data_warehouse',
    default_args=default_args,
    description='Load Data Warehouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['dw'],
    is_paused_upon_creation=False
)

wait_for_init = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',
    execution_date_fn = lambda x: datetime(2021, 1, 1, 0, 0, 0, 0, pytz.UTC),
    timeout=1,
    dag=dag
)


wait_for_oltp = ExternalTaskSensor(
    task_id='wait_for_oltp',
    external_dag_id='import_main_data',
    execution_date_fn= lambda x: get_most_recent_dag_run('import_main_data').execution_date,
    timeout=120,
    dag=dag
)

wait_for_flat_files = ExternalTaskSensor(
    task_id='wait_for_flat_files',
    external_dag_id='import_reseller_data',
    execution_date_fn = lambda x: get_most_recent_dag_run('import_reseller_data').execution_date,
    timeout=900,
    dag=dag
)

load_staging_resellers = PostgresOperator(
    task_id='load_staging_resellers',
    sql = 'sql/load_staging_resellers.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_staging_product = PostgresOperator(
    task_id='load_staging_product',
    sql = 'sql/load_staging_product.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_staging_customers = PostgresOperator(
    task_id='load_staging_customers',
    sql = 'sql/load_staging_customers.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_staging_channels = PostgresOperator(
    task_id='load_staging_channels',
    sql = 'sql/load_staging_channels.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_staging_transactions = PostgresOperator(
    task_id='load_staging_transactions',
    sql = 'sql/load_staging_transactions.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

dummy_load_warehouse = DummyOperator(task_id='load_warehouse_tables',dag=dag)

load_warehouse_geography = PostgresOperator(
    task_id='load_warehouse_geography',
    sql = 'sql/load_warehouse_geography.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


load_warehouse_salesagent = PostgresOperator(
    task_id='load_warehouse_salesagent',
    sql = 'sql/load_warehouse_salesagent.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_warehouse_product = PostgresOperator(
    task_id='load_warehouse_product',
    sql = 'sql/load_warehouse_product.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_warehouse_customer = PostgresOperator(
    task_id='load_warehouse_customer',
    sql = 'sql/load_warehouse_customer.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


load_warehouse_channel = PostgresOperator(
    task_id='load_warehouse_channel',
    sql = 'sql/load_warehouse_channel.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


load_warehouse_date = PostgresOperator(
    task_id='load_warehouse_date',
    sql = 'sql/load_warehouse_date.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

load_warehouse_sales = PostgresOperator(
    task_id='load_warehouse_sales',
    sql = 'sql/load_warehouse_sales.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

wait_for_init >> wait_for_oltp >> wait_for_flat_files >> load_warehouse_geography >> [load_staging_channels, load_staging_product, load_staging_resellers] >> dummy_load_warehouse

dummy_load_warehouse >> [ load_warehouse_salesagent,  load_warehouse_channel, load_warehouse_date] >> load_staging_customers 

load_staging_customers >> [load_warehouse_product, load_warehouse_customer] >> load_staging_transactions >> load_warehouse_sales