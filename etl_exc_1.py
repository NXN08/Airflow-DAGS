from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime as dtime
import pandas as pd

def_args = {
    'owner': "airflow",
    'start_date': dtime(2023, 1, 1)
    }

with DAG ('ETL',
          catchup = False,
          default_args = def_args) as dag:
    start = EmptyOperator(task_id = 'START')
    e= EmptyOperator(task_id = 'EXTRACT')
    t = EmptyOperator(task_id = 'TRANSFORM')
    l= EmptyOperator(task_id = 'LOAD')
    end = EmptyOperator(task_id = 'END')


start >> e >> t >> l >> end
    

