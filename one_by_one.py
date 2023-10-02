from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dtime
import os
import pandas as pd
import logging
import sys

# xl_file_path = '/home/user/sample_data/Efficiency_Report_16082023.xlsx'

add_path_to_sys = "/home/user/ETL_logics/XL_logics"
sys.path.append(add_path_to_sys)

from logics import *

def transform_data(data_dict, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=["EXTRACT"])
    # print(f'type of pulled data is:{type(xcom_pull_obj)}')
    data = pd.DataFrame(xcom_pull_obj)
    print(f'type of df is :{type(data)}')
    print(f'nononononononononoonononononon {data.shape}')
    def clean(data):
        clean_data = data.drop(['Unnamed: 0',
                            'Unnamed: 2',
                            'Unnamed: 4',
                            'Unnamed: 17',
                            'Unnamed: 21',
                            'Unnamed: 29',
                            'Unnamed: 117',
                            'Unnamed: 118'], axis=1)
        # clean_data_dict = clean_data.to_dict()
        return clean_data
    clean_data = clean(data)
    clean_data = pd.DataFrame(clean_data)
    print(f'hahahahahahahahahahah {clean_data.shape}')

# def transform_data(clean_data_dict, ti):
#     xcom_pull_obj = ti.xcom_pull(task_ids =["CLEAN"])
#     clean_data = pd.DataFrame(xcom_pull_obj)
#     print(f'this is they type of: {type(clean_data)}')
#     print(f'head of the clean_data is: {clean_data.head(5)}')

    def format_to_dt(row):
        if row['Unnamed: 1'] != 'Total' and row['Unnamed: 1'] != 'Average':
            return pd.to_datetime(str(row['Unnamed: 1']) + ' ' + str(row['Unnamed: 3']), errors='coerce')

    clean_data['datetime'] = clean_data.apply(format_to_dt, axis=1)

    def get_datetime_list(datetime_data):
        datetime_list = []
        r = 1
        while r <= 1440:
            datetime = datetime_data.iloc[r, 119]
            datetime_list.append(datetime)
            r += 1
        return datetime_list

    result_datetime_list = get_datetime_list(clean_data)

    def get_header_list(datetime_data):
        header_list = []
        h = 2
        while h <= 118:
            header = datetime_data.iloc[0, h]
            header = header.replace('\n', '')
            header = header.strip(' ')
            header_list.append(header)
            h += 1
        return header_list

    result_header_list = get_header_list(clean_data)

    def get_header_value_list(datetime_data):
        header_value_list = []
        for row_index in range(1, 1441):
            row_values = datetime_data.iloc[row_index, 2:119].tolist()
            header_value_list.append(row_values)
        return header_value_list

    result_header_value_list = get_header_value_list(clean_data)

    def gen_message_list(result_datetime_list, result_header_list, result_header_value_list):
        message_list = []
        for d in range(len(result_datetime_list)):
            datetime_key = result_datetime_list[d]
            message_dict = {}
            message_dict = {'Datetime': datetime_key}
            for v in range(len(result_header_list)):
                header_key = result_header_list[v]
                header_value = result_header_value_list[d][v]
                message_dict[header_key] = header_value
            message_list.append(message_dict)
        return message_list

    result_message_list = gen_message_list(
        result_datetime_list, result_header_list, result_header_value_list)

    return result_message_list


# --------------------------------------------------------------#
def_args = {
    'owner': "airflow",
    'start_date': dtime(2023, 1, 1),
    'schedule_interval': None,
}

with DAG('ONE-BY-ONE',
         catchup=False,
         default_args=def_args) as dag:
    start = EmptyOperator(task_id='START')

    read_xl_task = PythonOperator(
        task_id='EXTRACT',
        python_callable=read_xl)

    # clean_task = PythonOperator(
    #     task_id='CLEAN',
    #     python_callable=clean,
    #     op_args=["data_dict"])

    transformation_task = PythonOperator(
        task_id='TRANSFORMATION',
        python_callable=transform_data,
        op_args=["data_dict"])

start >> read_xl_task >> transformation_task
