from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dtime
import os
import pandas as pd
import logging
import sys
import json

# xl_file_path = '/home/user/sample_data/Efficiency_Report_16082023.xlsx'

add_path_to_sys = "/home/user/ETL_logics/XL_logics"
sys.path.append(add_path_to_sys)

from Excel_ETL_Final_logic import *


# --------------------------------------------------------------#
def_args = {
    'owner': "nixon",
    'start_date': dtime(2023, 1, 1),
    'schedule_interval': None,
}

with DAG('Excel_ETL_Final_DAG',
         catchup=False,
         default_args=def_args) as dag:
    start = EmptyOperator(task_id='START')

    read_xl_task = PythonOperator(
        task_id='EXTRACT',
        python_callable=read_xl,
        provide_context=True)

    transformation_task = PythonOperator(
        task_id='TRANSFORMATION',
        python_callable=transform_data,
        provide_context=True)
    
    load_task = PythonOperator(
    task_id='LOAD',
    python_callable=load_data,
    op_args=["result_msg_list"])

    end = EmptyOperator(task_id='END')


start >> read_xl_task >> transformation_task >> load_task >> end
