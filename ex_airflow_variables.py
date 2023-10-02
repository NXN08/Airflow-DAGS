from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

xyz = Variable.get("XYZ" , deserialize_json=True)
json_obj = Variable.get("DB_CONN_VARS")
dict_obj = Variable.get("DB_CONN_VARS", deserialize_json=True)
pwd = Variable.get("DB_PASSWORD")

def print_airflow_variables():
    print(f"the value of variable xyz is {xyz} and its datatype is {type(xyz)}")
    print(f"the value of variable json_obj is {json_obj} and its datatype is {type(json_obj)}")
    print(f"the value of variable dict_obj is {dict_obj} and its datatype is {type(dict_obj)}")
    print(f"the value of variable dict_obj['DB_HOST_IP'] is {dict_obj['DB_HOST_IP']}")
    print(f"the value of variable pwd is {pwd} and its datatype is {type(pwd)}")
    return None

def_args = {    "owner": "airflow",  "retries": 0 , "start_date": datetime(2021, 1, 1)} 
with DAG ("ex_airflow_variables",  default_args= def_args, catchup=False) as dag:
    start = DummyOperator(task_id = "START")
    airlow_variables = PythonOperator( task_id = "AIRFLOW_VARIABLES",   
        python_callable = print_airflow_variables  )
    end = DummyOperator(task_id = "END")

start >> airlow_variables >> end