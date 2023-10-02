import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 21),  # Set your desired start date
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Path where CSV files are located
xl_dir_path = '/home/user/sample_data'

# Name of the CSV file we are looking for
xl_file_name = 'Efficiency_Report_16082023.xlsx'

# Full path to CSV file
xl_file_path = os.path.join(xl_dir_path, xl_file_name)

# Data extraction
def read_xl(xl_file_path):
    if os.path.exists(xl_file_path):
        try:
            data = pd.read_excel(xl_file_path, 'Efficiency_Report')
            logging.info('Excel file read successfully.')
            return data
        except Exception as e:
            logging.error(f"Error reading Excel file: {str(e)}")
            return None
    else:
        logging.error(f"Error: File not found at the location.")
        return None

# Data cleaning
def clean(data):
    clean_data = data.drop(['Unnamed: 0',
                           'Unnamed: 2',
                           'Unnamed: 4',
                           'Unnamed: 17',
                           'Unnamed: 21',
                           'Unnamed: 29',
                           'Unnamed: 117',
                           'Unnamed: 118'], axis=1)
    return clean_data

# Data transformation
def format_to_dt(row):
    if row['Unnamed: 1'] != 'Total' and row['Unnamed: 1'] != 'Average':
        return pd.to_datetime(str(row['Unnamed: 1']) + ' ' + str(row['Unnamed: 3']), errors='coerce')

def transform_to_dt(clean_data):
    clean_data['datetime'] = clean_data.apply(format_to_dt, axis=1)
    return clean_data

# Get datetime list
def get_datetime_list(transformed_data):
    datetime_list = []
    r = 1
    while r <= 1440:
        datetime = transformed_data.iloc[r, 119]
        datetime_list.append(datetime)
        r += 1
    return datetime_list

# Get header list
def get_header_list(transformed_data):
    header_list = []
    h = 2
    while h <= 118:
        header = transformed_data.iloc[0, h]
        header = header.replace('\n', '')
        header = header.strip(' ')
        header_list.append(header)
        h += 1
    return header_list

# Get header value list
def get_header_value_list(transformed_data):
    header_value_list = []
    for row_index in range(1, 1441):
        row_values = transformed_data.iloc[row_index, 2:119].tolist()
        header_value_list.append(row_values)
    return header_value_list

# Generate message list
def gen_message_list(result_datetime_list, result_header_list, result_header_value_list):
    message_list = []
    for d in range(len(result_datetime_list)):
        datetime_key = result_datetime_list[d]
        message_dict = {'Datetime': datetime_key}
        for v in range(len(result_header_list)):
            header_key = result_header_list[v]
            header_value = result_header_value_list[d][v]
            message_dict[header_key] = header_value
        message_list.append(message_dict)
    return message_list

# Initialize the Airflow DAG
dag = DAG(
    'final-DAG',  
    default_args=default_args,
    description='ETL DAG for Excel data',
    schedule_interval=None,  
    catchup=False, 
)

# Define the ETL task
def run_etl():
    read_xl(xl_file_path)
    data = read_xl(xl_file_path)
    clean_data = clean(data)
    transformed_data = transform_to_dt(clean_data)
    result_datetime_list = get_datetime_list(transformed_data)
    result_header_list = get_header_list(transformed_data)
    result_header_value_list = get_header_value_list(transformed_data)
    result_message_list = gen_message_list(result_datetime_list, result_header_list, result_header_value_list)

# Task to run the ETL process
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

# Set task dependencies if you have additional tasks
# run_etl_task >> task2
# run_etl_task >> task3

if __name__ == "__main__":
    dag.cli()

