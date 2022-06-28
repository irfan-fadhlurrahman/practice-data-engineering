# ==============================================================================
# run the DAG with this command below and run in the directory ~/airflow/dags
# airflow tasks test <dag_id> <task_id> <date_in_the_past>
# i.e.: airflow task test first_airflow_dag get_datetime 2022-6-24
#
#
#
#
#
#
# ==============================================================================
import os
import pandas as pd
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def process_datetime(ti):
    date_time = ti.xcom_pull(task_ids=['get_datetime'])
    if not date_time:
        raise Exception('No datetime value.')

    # output from get_datetime: Sat Jun 25 14:53:58 +07 2022
    date_time = str(date_time[0]).split()
    return {
        "year": date_time[-1],
        "month": date_time[1],
        "day": date_time[2],
        "time": date_time[3],
        "day_of_week": date_time[0]
    }

def save_datetime(ti):
    datetime_processed = ti.xcom_pull(task_ids=['process_datetime'])
    if not datetime_processed:
        raise Exception('No processed datetime value.')

    # create a table
    df = pd.DataFrame(datetime_processed)

    # check Variable on the Airflow UI; Variable Key: first-dag-csv-path
    csv_path = Variable.get('first-dag-csv-path')
    if os.path.exists(csv_path):
        df_header = False
        df_mode = 'a'            # 'a': open for writing
    else:
        df_header = True
        df_mode = 'w'

    # pay attention to mode either 'a' or 'w'
    df.to_csv(csv_path, index=False, mode=df_mode, header=df_header)


with DAG(
    dag_id='first_airflow_dag',
    schedule_interval='* * * * *',
    start_date=datetime(year=2022, month=6, day=24),
    catchup=False
) as dag:

    # Task 1: get current datetime
    task_get_datetime = BashOperator(
        task_id='get_datetime',
        bash_command='date'
    )

    # Task 2: process current datetime
    task_process_datetime = PythonOperator(
        task_id='process_datetime',
        python_callable=process_datetime
    )

    # Task 3: save processed datetime
    task_save_datetime = PythonOperator(
        task_id='save_datetime',
        python_callable=save_datetime
    )

    # add the bit shift operator (>>) to connect each task
    task_get_datetime >> task_process_datetime >> task_save_datetime


























# ==============================================================================
