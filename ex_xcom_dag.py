from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

import pandas as pd

# All Constants be declared as Global Variable
GV = "K2 Analytics"

def extract_fn():
    print("Logic to Extract Data")
    print("Value of Global Variable Global Variable is: ", GV)
    rtn_val = "Analytics Training"
    return rtn_val
    
    # creating a Dataframe object 
    # details = {
    # 'cust_id' : [1, 2, 3, 4],
    # 'Name' : ['Rajesh', 'Jakhotia', 'K2', 'Analytics']
    # }
    # df = pd.DataFrame(details)
    # return df
    


# XCOMs: Cross - Communication
# ti stands for Task Instance
def transform_fn(a1, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids =["EXTRACT"])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    extract_rtn_obj = xcom_pull_obj[0]
    print(f"the value of xcom pull object is {extract_rtn_obj}")
    print("The value of a1 is ", a1) 
    print("Logic to Transform Data")
    return 10


def load_fn(p1, p2, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids =["EXTRACT"])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    print("the value of xcom pull object is {}".format(xcom_pull_obj[0]))
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to Load Data")


def_args = {
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "start_date": dtime(2022, 6,15)
} 
   

with DAG ("ex_xcom_push_pull",
         default_args= def_args,
          catchup=False) as dag:

    start = DummyOperator(task_id = "START")
    
    e = PythonOperator(
        task_id = "EXTRACT",
        python_callable = extract_fn
        # do_xcom_push=True  ## By default this parameter is set to True
    )


    t = PythonOperator(
        task_id = "TRANSFORM",
        python_callable = transform_fn,
        op_args = ["Learning Data Engineering with Airflow"],
        do_xcom_push=True
    )


    l = PythonOperator(
        task_id = "LOAD",
        python_callable = load_fn,
        op_args = ["K2", "Analytics"]
        #op_kwargs = {"p2" : "Analytics", "p1" : "K2"}
    )

    end = DummyOperator(task_id = "END")

start >> e >> t >> l >> end

# start.set_downstream(e)
# e.set_downstream(t)
# l.set_upstream(t)
# end.set_upstream(l)