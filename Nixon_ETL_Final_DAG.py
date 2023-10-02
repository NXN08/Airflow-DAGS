from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dtime
import sys

add_path_to_sys = "/home/user/ETL_logics/XL_logics"
sys.path.append(add_path_to_sys)

from Nixon_ETL_Final_logic import *
from archive_delete_logic import *

# --------------------------------------------------------------#
def_args = {
    'owner': "Nixon",
    'start_date': dtime(2023, 1, 1),
    'schedule_interval': None,
}

with DAG('Nixon_Final_DAG',
         catchup=False,
         default_args=def_args) as dag:
    start = EmptyOperator(task_id='START')

  # PythonOperators to runs the defined functions.
    read_config_task = PythonOperator(
        task_id='READ_CONFIG',
        python_callable=read_config,
        provide_context=True)

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
    
    archive_task = PythonOperator(
    task_id='ARCHIVE',
    python_callable=archive_files,
    dag=dag)

    delete_old_task = PythonOperator(
        task_id='DELETE_OLD_FILES',
        python_callable=delete_old_files,
        dag=dag)

    end = EmptyOperator(task_id='END')

# Task dependencies
start >> read_config_task >> read_xl_task >> transformation_task >> load_task >> archive_task >> delete_old_task >> end