from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import os
import re
import logging

from airflow.models import Variable


#### if we need to create the connection using python
# from sqlalchemy import create_engine
# from pandas.io import sql
# import psycopg2

# engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(
#     user=DB_USER,  pw=DB_PWD, ip=HOST_IP, db=DB_NAME, port=DB_PORT),
#     connect_args = {'options': '-csearch_path={}'.format(DB_SCHEMA)})
# conn_obj = engine.connect()


# Configuring the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Getting required airflow variables
file_configs = Variable.get("file_configs" , deserialize_json=True)


# accessing airflow variables
csv_file_path = file_configs['csv_file_path']
table_name = file_configs['table_name']
delimiter=file_configs['delimiter']



# Defining a Python function to get date, read the CSV file into a Pandas DataFrame and write it to PostgreSQL
def process_csv_file():
    logger.info(f"File to read - {csv_file_path}")

    # Extract the file name (file name without the directory)
    file_name = os.path.basename(csv_file_path)


    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file_path, delimiter=delimiter,skiprows=1, header=None,
        names=[
                'symbol', 'sc_name', 'sc_group', 'series',
                'open_price', 'high_price', 'low_price', 'close_price', 'last_price', 'prev_close_price',
                'no_of_trades', 'tot_trd_qty', 'tot_trd_val', 'tdcloindi',
                'isin', 'dt', 'filler2', 'filler3']
    ) 
    logger.info(f"File read sucessful")    
    

    logger.info("DataFrame ready:\n%s", df.head())    
    logger.info(f"Total rows present: {len(df)}")


    # using airflow connection
    pg_hook = PostgresHook(postgres_conn_id='pg_etl_demo')
    conn_engine = pg_hook.get_sqlalchemy_engine()
    
    # Writing the DataFrame to PostgreSQL
    df.to_sql(table_name, con=conn_engine, 
        if_exists='append', index=False)
    logger.info(f"Table write suceessfull Table - {table_name}") 
    return 1   

## DAG Related Code starts here

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 19),
}


dag = DAG(
    'csv_to_postgres_with_schema',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    max_active_runs=1,
)



# Creating dummy task 
load_csv_to_postgres = DummyOperator(task_id='load_csv_to_postgres', dag=dag)

# Define a PythonOperator task to process the CSV file
process_csv_task = PythonOperator(
    task_id='process_csv_task',
    python_callable=process_csv_file,
    provide_context=True,
    dag=dag,
)


load_csv_to_postgres >> process_csv_task
