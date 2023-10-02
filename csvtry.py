from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
from datetime import datetime as dtime
import os
import pandas as pd
import logging


# Path where excel files are located
xl_dir_path = '/home/user/sample_data'

# Name of the excel file we are looking for
xl_file_name = 'Efficiency_Report_16082023.xlsx'

# Full path to excel file
xl_file_path = os.path.join(xl_dir_path, xl_file_name)

def_args = {
    'owner': "airflow",
    'start_date': dtime(2023, 1, 1),
    'schedule_interval': '@daily',
}


# data extraction
def read_xl(xl_file_path):
    data = None
    try:
        data = pd.read_excel(xl_file_path, 'Efficiency_Report')
        logging.info('Excel file read successfully.')
    except FileNotFoundError as e:
        logging.error(f"File not found at the location: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred while reading the Excel file: {str(e)}")
        
    return data if data is not None else pd.DataFrame()  # Return an empty DataFrame if data is None

# checking if the excel file exists
def checks_xl_exist(xl_file_path):
    if os.path.exists(xl_file_path):
        read_xl(xl_file_path)
    else:
        logging.error('Error: No file found at the location.')

data = read_xl(xl_file_path)

# nan_count = data['Unnamed: 118'].isnull().sum()
# print(nan_count)

# data cleaning


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


clean_data = clean(data)

# nan_count = data['Unnamed: 118'].isnull().sum()
# print(nan_count)

# data transformation


def format_to_dt(row):
    if row['Unnamed: 1'] != 'Total' and row['Unnamed: 1'] != 'Average':
        # errors='coerce' convert the invalid values to NaT
        return pd.to_datetime(str(row['Unnamed: 1']) + ' ' + str(row['Unnamed: 3']), errors='coerce')


def transform_to_dt(clean_data):
    clean_data['datetime'] = clean_data.apply(format_to_dt, axis=1)
    return clean_data


transformed_data = transform_to_dt(clean_data)

# dtime = clean_data.iloc[1,119]


def get_datetime_list(transformed_data):
    datetime_list = []
    r = 1
    while r <= 1440:
        datetime = transformed_data.iloc[r, 119]
        datetime_list.append(datetime)
        r += 1

    return datetime_list


result_datetime_list = get_datetime_list(transformed_data)


# header = clean_data.iloc[0,2]
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


result_header_list = get_header_list(transformed_data)


# header_value = clean_data.iloc[1,2]
def get_header_value_list(transformed_data):
    header_value_list = []
    for row_index in range(1, 1441):
        row_values = transformed_data.iloc[row_index, 2:119].tolist()
        header_value_list.append(row_values)

    return header_value_list


result_header_value_list = get_header_value_list(transformed_data)


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


# -------------------------------------------------------------- #


# with DAG('XL_ETL',
#          catchup=False,
#          default_args=def_args) as dag:
#     check_xl_exist_task = PythonOperator(
#         task_id='EXTRACT',
#         python_callable=checks_xl_exist)

#     clean_task = PythonOperator(
#         task_id='CLEAN',
#         python_callable=clean)

#     transform_to_dt_task = PythonOperator(
#         task_id='TRANSFORMATION',
#         python_callable=transform_to_dt)

#     gen_message_list_task = PythonOperator(
#         task_id='MESSAGE',
#         python_callable=gen_message_list)


# check_xl_exist_task >> clean_task >> transform_to_dt_task >> gen_message_list_task
