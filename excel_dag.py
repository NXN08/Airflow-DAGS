from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dtime
import os
import pandas as pd
import logging

def_args = {
    'owner': "airflow",
    'start_date': dtime(2023, 1, 1),
    'schedule_interval': None,
}

xl_dir_path = '/home/user/sample_data'
xl_file_name = 'Efficiency_Report_16082023.xlsx'
xl_file_path = os.path.join(xl_dir_path, xl_file_name)


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

# def format_to_dt(row):
#     if row['Unnamed: 1'] != 'Total' and row['Unnamed: 1'] != 'Average':
#         return pd.to_datetime(str(row['Unnamed: 1']) + ' ' + str(row['Unnamed: 3']), errors='coerce')

# def transform_to_dt(clean_data):
#     clean_data['datetime'] = clean_data.apply(format_to_dt, axis=1)
#     return clean_data


def merge_format_and_transform(clean_data):
    def format_to_dt(row):
        if row['Unnamed: 1'] != 'Total' and row['Unnamed: 1'] != 'Average':
            return pd.to_datetime(str(row['Unnamed: 1']) + ' ' + str(row['Unnamed: 3']), errors='coerce')

    clean_data['datetime'] = clean_data.apply(format_to_dt, axis=1)
    return clean_data


def get_datetime_list(transformed_data):
    datetime_list = []
    for r in range(1, 1441):
        datetime = transformed_data.iloc[r, 119]
        datetime_list.append(datetime)
    return datetime_list

def get_header_list(transformed_data):
    header_list = []
    for h in range(2, 119):
        header = transformed_data.iloc[0, h]
        header = header.replace('\n', '')
        header = header.strip(' ')
        header_list.append(header)
    return header_list

def get_header_value_list(transformed_data):
    header_value_list = []
    for row_index in range(1, 1441):
        row_values = transformed_data.iloc[row_index, 2:119].tolist()
        header_value_list.append(row_values)
    return header_value_list

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

with DAG('EXCEL-ETL',
         catchup=False,
         default_args=def_args) as dag:
    start = EmptyOperator(task_id='START')

    read_xl_task = PythonOperator(
        task_id='EXTRACT',
        python_callable=read_xl)
    
    clean_task = PythonOperator(
        task_id='CLEAN',
        python_callable=clean,
        op_args=[read_xl_task.output])
    
    transform_to_dt_task = PythonOperator(
        task_id='TRANSFORMATION',
        python_callable=transform_to_dt,
        op_args=[clean_task.output])
    
    gen_message_list_task = PythonOperator(
        task_id='MESSAGE',
        python_callable=gen_message_list,
        op_args=[transform_to_dt_task.output])

start >> read_xl_task >> clean_task >> transform_to_dt_task >> gen_message_list_task

