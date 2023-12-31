from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import sys

add_path_to_sys = "/home/user/py_scripts2"
sys.path.append(add_path_to_sys)

from init import * 
from etl_biz_logic import * 

def_args = {    "owner": "airflow",  "retries": 0, 
                "retry_delay": timedelta(minutes=1),
                "start_date": dtime(2022, 6,15) } 

with DAG ("ex_modular_dag_2",  default_args= def_args, catchup=False) as dag:

    start = DummyOperator(task_id = "START")
    
    e = PythonOperator( task_id = "EXTRACT",   python_callable = extract_fn    )

    t = PythonOperator( task_id = "TRANSFORM", python_callable = transform_fn,
        op_args = ["Learning Data Engineering with Airflow"], do_xcom_push=True    )

    l = PythonOperator( task_id = "LOAD",      python_callable = load_fn,
        op_args = ["K2", "Analytics"])       #op_kwargs = {"p2" : "Analytics", "p1" : "K2"}
    
    end = DummyOperator(task_id = "END")

start >> e >> t >> l >> end