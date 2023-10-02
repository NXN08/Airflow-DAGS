
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import XCom
from datetime import datetime
import pandas as pd
import os
import re
import logging

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Defining the schema and config table name 
table_name = 'config_table'

# using airflow connection
pg_hook = PostgresHook(postgres_conn_id='pg_etl_demo')
conn_engine = pg_hook.get_sqlalchemy_engine()


# Define a Python function to read the config_table and store the values in XCom
def read_config_table(**kwargs):
    try:

        # Query the config_table to get CSV file information
        query = f"""SELECT file_location, file_name, delimiter, target_table 
            FROM {table_name}"""
        results = pg_hook.get_records(sql=query)

        if len(results) > 0:
            # Assuming that the query returns a single row with csv_path, delimiter, and target
            file_location,file_name, delimiter, target_table = results[0]
            csv_file_path = file_location+file_name
            logger.info(f"""CSV Path: {csv_file_path}, Delimiter: {delimiter}, 
                Target: {target_table}""")
            
            # Store the values in XCom for retrieval by downstream tasks
            kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)
            kwargs['ti'].xcom_push(key='delimiter', value=delimiter)
            kwargs['ti'].xcom_push(key='target', value=target_table)
        else:
            logger.error("No data found in config_table.")
    except Exception as e:
        logger.error(f"Error reading config_table: {str(e)}")

# Define a Python function to read the CSV file using values retrieved from XCom
def read_csv_file(**kwargs):
    try:
        # Retrieve values from XCom
        csv_file_path = kwargs['ti'].xcom_pull(
            task_ids='read_config_task', key='csv_file_path')
        delimiter = kwargs['ti'].xcom_pull(task_ids='read_config_task', key='delimiter')
        table_name = kwargs['ti'].xcom_pull(task_ids='read_config_task', key='target')
        
        # Load the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_file_path, delimiter=delimiter,skiprows=1, header=None,
            names=[
                    'symbol', 'sc_name', 'sc_group', 'series',
                    'open_price', 'high_price', 'low_price', 'close_price', 'last_price', 'prev_close_price',
                    'no_of_trades', 'tot_trd_qty', 'tot_trd_val', 'tdcloindi',
                    'isin', 'dt', 'filler2', 'filler3']
        ) 
        logger.info(f"File read sucessful")    
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        return None        

        logger.info("DataFrame ready:\n%s", df.head())    
        logger.info(f"Total rows present: {len(df)}")
    
    try:
        
        # Writing the DataFrame to PostgreSQL
        df.to_sql(table_name, con=conn_engine, 
            if_exists='append', index=False)
        logger.info(f"Table write suceessfull Table - {table_name}") 
        conn_engine.dispose()
        return 1   

    except Exception as e:
        logger.error(f"Error Writing data to Table -{table_name}")
        return None        


# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 19),
}

# Create the DAG
dag = DAG(
    'csv_to_postgres_with_schema_2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

# Creating a dummy task
load_csv_to_postgres = DummyOperator(task_id='load_csv_to_postgres', dag=dag)


# Define three PythonOperator tasks
read_config_task = PythonOperator(
    task_id='read_config_task',
    python_callable=read_config_table,
    provide_context=True,
    dag=dag,
)

read_csv_task = PythonOperator(
    task_id='read_csv_task',
    python_callable=read_csv_file,
    provide_context=True,
    dag=dag,
)



# Set task dependencies
load_csv_to_postgres >> read_config_task >> read_csv_task

if __name__ == "__main__":
    dag.cli()
