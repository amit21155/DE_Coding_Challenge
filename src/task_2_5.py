from src import task1, task2, task3, task4, task5, task6

import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': dt.datetime(2021, 3, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('airflow_tutorial_v01',
         default_args=default_args,
         schedule_interval='0 0 * * *',
         ) as dag:

    task1 = PythonOperator(task_id='task1', python_callable=task1)

    task2 = PythonOperator(task_id='task2', python_callable=task2)

    task3 = PythonOperator(task_id='task3', python_callable=task3)
	
	task4 = PythonOperator(task_id='task4', python_callable=task4)
	
	task5 = PythonOperator(task_id='task5', python_callable=task5)
	
	task6 = PythonOperator(task_id='task6', python_callable=task6)

    

task1>>task2
task1>>task3
task3>>task4
task3>>task5
task3>>task6
