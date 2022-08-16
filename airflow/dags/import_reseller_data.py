
from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import os
import pandas as pd
import json
import xmltodict


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
    'import_reseller_data',
    default_args=default_args,
    description='Import Resellers Transactions Files',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['csv','reseller'],
    is_paused_upon_creation=False
)

POSTGRES_HOOK = PostgresHook('sales_dw')
ENGINE = POSTGRES_HOOK.get_sqlalchemy_engine()

wait_for_init = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',
    execution_date_fn = lambda x: datetime(2021, 1, 1, 0, 0, 0, 0, pytz.UTC),
    timeout=1,
    dag=dag
)

def get_validated(filetype):

    with ENGINE.connect() as con:

        result = con.execute(f"""SELECT Filename FROM ops.FlatFileLoadRegistry where validated=True and extension='{filetype}' """)

        return set(row.values()[0] for row in result)


def get_processed(filetype):

    with ENGINE.connect() as con:

        result = con.execute(f"""SELECT Filename FROM ops.FlatFileLoadRegistry where processed=True and extension='{filetype}' """)

        return set(row.values()[0] for row in result)


def update_flatfile_registry(file_data):

    command = f"""
    INSERT INTO ops.FlatFileLoadRegistry(Filename, Extension, LoadDate, Processed, Validated)
    VALUES('{file_data['filename']}','{file_data['extension']}','{file_data['loaddate']}',{file_data['processed']}, {file_data['validated']} ) 
    ON CONFLICT (Filename) 
    DO UPDATE SET processed={file_data['processed']}, validated={file_data['validated']}, loaddate='{file_data['loaddate']}';
    """

    with ENGINE.connect() as con:

        con.execute(command)

def preprocess_csv():

    IMPORT_PATH = '/import/csv/raw/'
    EXPORT_PATH = '/import/csv/processed/'
    PROCESSED = get_processed('csv')
    IGNORED = '.keep'
    
    for file in sorted(os.listdir(IMPORT_PATH)):

        if file not in PROCESSED and file != IGNORED:
            extension = file.split('.')[-1]

            df = pd.read_csv(IMPORT_PATH+file)

            df['Imported_File'] = file

            df.to_csv(EXPORT_PATH+file,index=False)

            file_data = {'filename': file, 'extension': extension, 'loaddate': datetime.now() ,'processed':True, 'validated': False}
            
            update_flatfile_registry(file_data)
            
            print(f'Processed {file}')



def import_csv():

    PATH = '/import/csv/processed'
    VALIDATED = get_validated('csv')
    IGNORED = '.keep'

    for file in sorted(os.listdir(PATH)):

        if file not in VALIDATED and file != IGNORED:

            extension = file.split('.')[-1]

            SQL_STATEMENT = """
            COPY import.ResellerCSV(Transaction_ID, Product_name, Number_of_purchased_postcards, Total_amount, Sales_Channel, Customer_First_Name, Customer_Last_Name, Customer_Email, Office_Location, Created_Date, Imported_File) FROM STDIN DELIMITER ',' CSV HEADER;
            """
            conn = POSTGRES_HOOK.get_conn()
            cur = conn.cursor()

            with open(PATH+'/'+file, 'r') as f:
                cur.copy_expert(SQL_STATEMENT, f)
                conn.commit()
            
            file_data = {'filename': file, 'extension': extension, 'loaddate': datetime.now() ,'processed': True, 'validated': True}
            
            update_flatfile_registry(file_data)
            
            print(f'Imported {file}')



def preprocess_xml():

    IMPORT_PATH = '/import/xml/raw/'
    EXPORT_PATH = '/import/xml/processed/'
    IGNORED = '.keep'
    
    PROCESSED = get_processed('xml')
    
    for file in sorted(os.listdir(IMPORT_PATH)):

        if file not in PROCESSED and file != IGNORED:

            with open(IMPORT_PATH + file, 'r') as myfile:
                obj = xmltodict.parse(myfile.read())
            
            if obj['transactions']:

                with open(EXPORT_PATH + file[:-4] +'.json', 'w', encoding='utf-8') as f:
                    for e in dict(obj['transactions'])['transaction']:
                        json.dump(e, f, ensure_ascii=False)
                        f.write('\n')

                file_data = {'filename': file, 'extension': 'xml', 'loaddate': datetime.now() ,'processed':True, 'validated': False}
            else:
                file_data = {'filename': file, 'extension': 'xml', 'loaddate': datetime.now() ,'processed':False, 'validated': False}

            update_flatfile_registry(file_data)

        print(f'Processed {file}')

def import_xml():

    PATH = '/import/xml/processed'
    VALIDATED = get_validated('xml')
    IGNORED = '.keep'

    for file in sorted(os.listdir(PATH)):

        if file not in VALIDATED and file != IGNORED:

            SQL_STATEMENT = """
            COPY import.ResellerXML(data) FROM STDIN;
            """
            conn = POSTGRES_HOOK.get_conn()
            cur = conn.cursor()

            with open(PATH+'/'+file, 'r') as f:
                cur.copy_expert(SQL_STATEMENT, f)
                conn.commit()
            
            filename = file[:-4]
            xml_filename = filename+'.xml'

            file_data = {'filename': xml_filename, 'extension': 'xml', 'loaddate': datetime.now() ,'processed': True, 'validated': True}
            
            update_flatfile_registry(file_data)
            
            print(f'Imported {xml_filename}({file})')

preprocess_csv = PythonOperator(
    task_id='preprocess_csv',
    python_callable=preprocess_csv,
    dag=dag
)

import_csv = PythonOperator(
    task_id='import_csv',
    python_callable=import_csv,
    dag=dag
)

preprocess_xml = PythonOperator(
    task_id='preprocess_xml',
    python_callable=preprocess_xml,
    dag=dag
)

import_xml = PythonOperator(
    task_id='import_xml',
    python_callable=import_xml,
    dag=dag
)

create_transform_reseller_destination = PostgresOperator(
    task_id='create_transform_reseller_destination',
    sql = """
        CREATE TABLE IF NOT EXISTS staging.ResellerXmlExtracted (
        reseller_id int,
        customer_first_name varchar(255),
        customer_last_name varchar(255),
        customer_email varchar(255),
        product_name varchar(255),
        date_bought date,
        sales_channel varchar(255),
        total_amount money,
        transaction_id int,
        no_purchased_postcards int,
        file_date date,
        Office_location varchar(255),
        Loaded_Timestamp timestamp not null default now()
        )
    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

insert_transform_reseller = PostgresOperator(
    task_id='insert_transform_reseller',
    sql = """
        INSERT INTO staging.ResellerXmlExtracted (
        reseller_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        product_name,
        date_bought,
        sales_channel,
        total_amount,
        transaction_id,
        no_purchased_postcards,
        file_date,
        office_location
        )

        select 
        cast(data ->> '@reseller-id' as int) as reseller_id,
        data -> 'customer'->> 'firstname' as customer_first_name,
        data -> 'customer'->> 'lastname' as customer_last_name,
        data -> 'customer'->> 'email' as customer_email,
        data ->> 'productName' as product_name,
        to_date(data ->> 'dateCreated','YYYYMMDD') as date_bought,
        data ->> 'salesChannel' as sales_channel,
        cast(data ->> 'totalAmount' as money) as total_amount,
        cast(data ->> 'transactionId' as int) as transaction_id,
        cast(data ->> 'qty' as int) as no_purchased_postcards,
        to_date(data ->> '@date','YYYYMMDD') as file_date,
        data ->> 'officeLocation' as Office_location

        from import.resellerxml

    """,
    dag=dag,
    postgres_conn_id = 'sales_dw',
    autocommit = True
)

create_transform_reseller_destination >> insert_transform_reseller

wait_for_init >> [preprocess_csv, preprocess_xml]
preprocess_csv >> import_csv
preprocess_xml >> import_xml
import_xml >> create_transform_reseller_destination >> insert_transform_reseller