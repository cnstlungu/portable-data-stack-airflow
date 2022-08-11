
from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

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
    'initialize_etl_environment',
    default_args=default_args,
    description='Initialize ETL Environment',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1, 0, 0, 0, 0, pytz.UTC),
    tags=['init'],
    is_paused_upon_creation=False
)


create_schemas_task = PostgresOperator(
    task_id='create_schemas',
    sql = """
    Create schema if not exists import;
    Create schema if not exists warehouse;
    Create schema if not exists ops;        
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_registry_table = PostgresOperator(
    task_id='create_registry_table',
    sql = """
    CREATE TABLE IF NOT EXISTS ops.FlatFileLoadRegistry (
        EntryID serial PRIMARY KEY, 
        Filename varchar(255) UNIQUE, 
        Extension varchar(10), 
        LoadDate timestamp,
        Processed boolean, 
        Validated boolean);
        
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_csv_destination = PostgresOperator(
    task_id='create_csv_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.ResellerCSV (
   
   Transaction_ID int , 
   Product_name varchar(255),
   Number_of_purchased_postcards int,
   Total_amount money,
   Sales_Channel varchar(255),
   Customer_First_Name varchar(255),
   Customer_Last_Name varchar(255),
   Customer_Email varchar(255),
   Office_Location varchar(255),
   Created_Date date,
   Imported_File varchar(255),
   Loaded_Timestamp timestamp not null default now()
   
   );
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_xml_destination_task = PostgresOperator(
    task_id='create_xml_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.ResellerXML (
   
   EntryID serial PRIMARY KEY,
   Data jsonb,
   Loaded_Timestamp timestamp not null default now()
   );
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_transactions_task = PostgresOperator(
    task_id='create_transactions_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.transactions(
    customer_id int, 
    product_id int, 
    amount money, 
    qty int, 
    channel_id int, 
    bought_date date,
    transaction_id int,
    Loaded_Timestamp timestamp not null default now()
    )
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_resellers_destination_task = PostgresOperator(
    task_id='create_resellers_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.resellers(reseller_id int, reseller_name VARCHAR(255), commission_pct decimal, Loaded_Timestamp timestamp not null default now())
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_channels_destination_task = PostgresOperator(
    task_id='create_channels_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.channels(channel_id int, channel_name VARCHAR(255), Loaded_Timestamp timestamp not null default now())
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_customers_destination_task = PostgresOperator(
    task_id='create_customers_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.customers(customer_id int, first_name VARCHAR(255), email varchar(255), last_name VARCHAR(255), Loaded_Timestamp timestamp not null default now())
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


create_products_destination_task = PostgresOperator(
    task_id='create_products_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.products(product_id int, name VARCHAR(255), city VARCHAR(255), price money, Loaded_Timestamp timestamp not null default now())
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_schemas_task >> create_registry_table >> [create_csv_destination, create_xml_destination_task]

create_schemas_task >> [create_transactions_task,create_resellers_destination_task,create_channels_destination_task, create_customers_destination_task, create_products_destination_task]
