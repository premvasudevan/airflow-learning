from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.operators 

def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('hello_world_v2', description='Hello World DAG V2 after creating winenv and using py interpretter',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator