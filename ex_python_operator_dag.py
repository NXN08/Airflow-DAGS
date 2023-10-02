from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


def extract_fn():
    print("Logic to Extract Data")
    return "some object"


# XCOMs - Cross - Communication
def transform_fn(a1):
    print("The value of a1 is ", a1) 
    print("Logic to Transform Data")


def load_fn(p1, p2):
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to Load Data")

def etl_fn(a1, p1, p2):
    print("Logic to Extract Data")
    print("The value of a1 is ", a1) 
    print("Logic to Transform Data")
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to Load Data")


def_args = {
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "start_date": dtime(2022, 6,15)
} 
   
# Scheduler Options
# Daily at what time , Monthly, Hourly, On a specifi date every month

# timezone=timezone('Asia/Kolkata')     # Set the timezone to IST
# schedule_interval='15 * * * *'        # 1:15 AM, 2:15 AM, 3:15 AM, etc.
# schedule_interval='30 18 * * *',      # 6:30 PM IST daily
# schedule_interval='0 10 * * WED'      # 10:00 AM on Wednesdays
# schedule_interval='0 14 17 * *'       # 17th day of every month at 2:00 PM
# schedule_interval='0 7 1 3 *'         # 7:00 AM on March 1st of each year


with DAG ("ex_py_operator",
         default_args= def_args,
          catchup=False) as dag:

    start = DummyOperator(task_id = "START")
    
    e = PythonOperator(
        task_id = "ETRACT",
        python_callable = extract_fn
    )

# op_args -> Operator Arguments
    t = PythonOperator(
        task_id = "TRANSFORM",
        python_callable = transform_fn,
        op_args = ["Learning Data Engineering with Airflow"]
    )

# op_args -> Operator Arguments
    l = PythonOperator(
        task_id = "LOAD",
        python_callable = load_fn,
        #op_args = ["K2", "Analytics"]
        op_kwargs = {"p2" : "Analytics", "p1" : "K2"}
    )

    end = DummyOperator(task_id = "END")

start >> e >> t >> l >> end

##### Command to run the dag from terminal
# airflow tasks test <dag_id> <task_id> <execution_date>
# airflow tasks test ex_py_operator EXTRACT 2022-6-17
# airflow tasks test ex_py_operator TRANSFORM 2022-6-17
# airflow tasks test ex_py_operator LOAD 2022-6-17
