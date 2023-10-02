from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

root_folder_path = "/home/user/py_scripts2"
os.chdir(root_folder_path)
exec(open('./init.py').read())
exec(open('./db_conn.py').read())
exec(open('./etl_db_data.py').read())

def_args = {    "owner": "airflow",  "retries": 0, 
                "retry_delay": timedelta(minutes=1),    "start_date": dtime(2022, 6,15) } 

with DAG ("ex_database_conn",  default_args= def_args, catchup=False) as dag:

    start = DummyOperator(task_id = "START")
    e_t_l = PythonOperator( task_id = "EXTRACT_TRANSFORM_LOAD",   
    python_callable = etl  )
    end = DummyOperator(task_id = "END")

start >> e_t_l >> end