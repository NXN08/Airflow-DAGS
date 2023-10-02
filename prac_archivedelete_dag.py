from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
from datetime import datetime, timedelta
import os
import glob

# Define your DAG with appropriate settings.
dag = DAG(
    'archive_delete_dag',
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # Set to False if you don't want to backfill
    default_args={
        'owner': 'Nixon',
        'start_date': '2023-01-01'  
    }
)

# Function to archive and delete CSV files older than three months.
def archive_files():
    # Read configuration from the existing JSON configuration file (config.json).
    with open('/home/user/ETL_logics/XL_logics/Nixon_config.json', 'r') as config_file:
        config = json.load(config_file)

    # Get the file location and archive location from the configuration.
    folder_location = config['EXCEL_folder']['folder_location']
    archive_location = config['Archive']['archive_location']

    # Calculate the date three months ago.
    today = datetime.now()
    three_months_ago = today - timedelta(days=90)  # 90 days = 3 months

    # Loop through CSV files in the specified directory.
    for excel_file in glob.glob(os.path.join(folder_location, '*.xlsx')):
        file_mtime = datetime.fromtimestamp(os.path.getmtime(excel_file))
        if file_mtime <= three_months_ago:
            # If lesser than three months, archive the file to the archive folder.
            archive_file_path = os.path.join(archive_location, os.path.basename(excel_file))
            os.rename(excel_file, archive_file_path)


def delete_old_files():
    # Read configuration from the existing JSON configuration file (config.json).
    with open('/home/user/ETL_logics/XL_logics/Nixon_config.json', 'r') as config_file:
        config = json.load(config_file)

    # Get the archive location from the configuration.
    archive_location = config['Archive']['archive_location']

    # Calculate the date three months ago.
    today = datetime.now()
    three_months_ago = today - timedelta(days=90)  # 90 days = 3 months

    # Loop through archived CSV files in the specified archive location.
    for archived_excel_file in glob.glob(os.path.join(archive_location, '*.xlsx')):
        file_mtime = datetime.fromtimestamp(os.path.getmtime(archived_excel_file))
        if file_mtime > three_months_ago:
            # If older than three months, delete the archived file.
            os.remove(archived_excel_file)

# Create a PythonOperator that runs the archive_and_delete_files function.
archive_task = PythonOperator(
    task_id='archive_task',
    python_callable=archive_files,
    dag=dag
)

delete_old_task = PythonOperator(
    task_id='delete_old_task',
    python_callable=delete_old_files,
    dag=dag
)

# Set up task dependencies.
archive_task >> delete_old_task