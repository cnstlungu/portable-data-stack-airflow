
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
    Create schema if not exists staging;
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



dummy_create_staging = DummyOperator(task_id='create_staging_tables',dag=dag)



create_staging_resellers = PostgresOperator(
    task_id='create_staging_resellers',
    sql = """

    CREATE TABLE IF NOT EXISTS staging.resellers    
    (
    reseller_id int,
    reseller_name varchar(255),
    commission_pct decimal
    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)



create_staging_product = PostgresOperator(
    task_id='create_staging_product',
    sql = """

    CREATE TABLE IF NOT EXISTS staging.product
    (
    product_id int,
    name varchar(255),
    geographykey int,
    price money

    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)




create_staging_customers = PostgresOperator(
    task_id='create_staging_customers',
    sql = """

    CREATE TABLE IF NOT EXISTS staging.customers
    (customer_id varchar(10), 
    first_name varchar(255), 
    last_name varchar(255),
    email varchar(255), 
    salesagentkey int);

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


create_staging_channels = PostgresOperator(
    task_id='create_staging_channels',
    sql = """
    CREATE TABLE IF NOT EXISTS staging.channels
    (
    channel_id int,
    channel_name varchar(255)
    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)



create_staging_transactions = PostgresOperator(
    task_id='create_staging_transactions',
    sql = """
    CREATE TABLE  IF NOT EXISTS staging.transactions
    (
    Customer_Key int,
    Transaction_ID int,
    ProductKey int,
    ChannelKey int,
    SalesAgentKey int,
    GeographyKey int,
    BoughtDateKey int,
    Total_Amount money,
    Qty int,
    Price money,
    CommissionPaid money,
    CommissionPct decimal
    );



    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)



create_warehouse_salesagent = PostgresOperator(
    task_id='create_warehouse_salesagent',
    sql = """
    CREATE TABLE IF NOT EXISTS warehouse.Dim_SalesAgent (

    SalesAgentKey serial primary key,
    OriginalResellerId int unique,
    ResellerName varchar(255),
    CommissionPct decimal,
    Loaded_Timestamp timestamp not null default now()
    );


    insert into warehouse.dim_salesagent(salesagentkey, OriginalResellerId, resellername)
    select -1, -1, 'Direct Sales'
    on conflict(salesagentkey) do nothing;

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_warehouse_geography = PostgresOperator(
    task_id='create_warehouse_geography',
    sql = """
    CREATE TABLE IF NOT EXISTS warehouse.Dim_Geography (
    GeographyKey serial primary key,
    CityName varchar(255) unique,
    CountryName varchar(255),
    RegionName varchar(255),
    Loaded_Timestamp timestamp not null default now()
    ) ;


    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


create_warehouse_product = PostgresOperator(
    task_id='create_warehouse_product',
    sql = """
    CREATE TABLE IF NOT EXISTS warehouse.Dim_Product (
    ProductKey serial primary key,
    OriginalProductID int,
    ProductName varchar(255),
    GeographyKey int,
    Price money,
    Loaded_Timestamp timestamp not null default now(),
    CONSTRAINT fk_product_geography
    FOREIGN KEY(GeographyKey) 
    REFERENCES warehouse.dim_geography(GeographyKey)

    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)



create_warehouse_customer = PostgresOperator(
    task_id='create_warehouse_customer',
    sql = """
    CREATE TABLE IF NOT EXISTS warehouse.Dim_Customer (
    CustomerKey serial primary key,
    OriginalCustomerID varchar(32),
    CustomerFirstName varchar(255),
    CustomerLastName varchar(255),
    CustomerEmail varchar(255),
    SalesAgentKey int,
    Loaded_Timestamp timestamp not null default now(),
    CONSTRAINT fk_customer_salesagent
    FOREIGN KEY(SalesAgentKey) 
    REFERENCES warehouse.dim_salesagent(SalesAgentKey)


    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)


create_warehouse_channel = PostgresOperator(
    task_id='create_warehouse_channel',
    sql = """

    CREATE TABLE IF NOT EXISTS warehouse.Dim_Channel (

    ChannelKey serial primary key,
    OriginalChannelID int,
    ChannelName varchar(255),
    Loaded_Timestamp timestamp not null default now()


    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_warehouse_date = PostgresOperator(
    task_id='create_warehouse_date',
    sql = """
    CREATE TABLE IF NOT EXISTS warehouse.Dim_Date
    (
    DateKey              INT NOT NULL PRIMARY KEY,
    date_actual              DATE NOT NULL,
    epoch                    BIGINT NOT NULL,
    day_suffix               VARCHAR(4) NOT NULL,
    day_name                 VARCHAR(9) NOT NULL,
    day_of_week              INT NOT NULL,
    day_of_month             INT NOT NULL,
    day_of_quarter           INT NOT NULL,
    day_of_year              INT NOT NULL,
    week_of_month            INT NOT NULL,
    week_of_year             INT NOT NULL,
    week_of_year_iso         CHAR(10) NOT NULL,
    month_actual             INT NOT NULL,
    month_name               VARCHAR(9) NOT NULL,
    month_name_abbreviated   CHAR(3) NOT NULL,
    quarter_actual           INT NOT NULL,
    quarter_name             VARCHAR(9) NOT NULL,
    year_actual              INT NOT NULL,
    first_day_of_week        DATE NOT NULL,
    last_day_of_week         DATE NOT NULL,
    first_day_of_month       DATE NOT NULL,
    last_day_of_month        DATE NOT NULL,
    first_day_of_quarter     DATE NOT NULL,
    last_day_of_quarter      DATE NOT NULL,
    first_day_of_year        DATE NOT NULL,
    last_day_of_year         DATE NOT NULL,
    mmyyyy                   CHAR(6) NOT NULL,
    mmddyyyy                 CHAR(10) NOT NULL,
    weekend_indr             BOOLEAN NOT NULL
    );
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)




create_warehouse_sales = PostgresOperator(
    task_id='create_warehouse_sales',
    sql = """
    CREATE TABLE IF NOT EXISTS warehouse.Fact_Sales (
    SalesKey serial PRIMARY KEY,
    OriginalTransactionID int,
    CustomerKey int,
    ProductKey int,
    ChannelKey int,
    SalesAgentKey int,
    BoughtDateKey int,
    GeographyKey int,
    TotalAmount money,
    NbrBoughtPostcards int,
    CommissionPct decimal,
    CommissionPaid money,
    Price money,
    Loaded_Timestamp timestamp not null default now(),
    CONSTRAINT fk_sales_customer
    FOREIGN KEY(CustomerKey) 
    REFERENCES warehouse.dim_customer(CustomerKey),
    CONSTRAINT fk_sales_product
    FOREIGN KEY(ProductKey) 
    REFERENCES warehouse.dim_product(ProductKey),
    CONSTRAINT fk_sales_channel
    FOREIGN KEY(ChannelKey) 
    REFERENCES warehouse.dim_channel(ChannelKey),
    CONSTRAINT fk_sales_salesagent
    FOREIGN KEY(SalesAgentKey) 
    REFERENCES warehouse.dim_salesagent(SalesAgentKey),
    CONSTRAINT fk_sales_boughtdate
    FOREIGN KEY(BoughtDateKey) 
    REFERENCES warehouse.dim_Date(DateKey),
    CONSTRAINT fk_sales_geography
    FOREIGN KEY(GeographyKey) 
    REFERENCES warehouse.dim_Geography(GeographyKey)
    
    );

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)



create_vw_top_sales_region = PostgresOperator(
    task_id='create_vw_top_sales_region',
    sql = 'sql/create_vw_top_sales_region.sql',
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

dummy_create_staging >> [create_staging_resellers, create_staging_product, create_staging_customers, create_staging_channels, create_staging_transactions] >> create_warehouse_geography

create_schemas_task >> create_registry_table >> [create_csv_destination, create_xml_destination_task]

create_schemas_task >> [create_transactions_task,create_resellers_destination_task,create_channels_destination_task, create_customers_destination_task, create_products_destination_task] >> dummy_create_staging

create_warehouse_geography >> create_warehouse_date >> create_warehouse_salesagent >> [create_warehouse_channel, create_warehouse_customer , create_warehouse_product] >> create_warehouse_sales >> create_vw_top_sales_region
