import json
import pendulum
from airflow import DAG

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

with DAG (
    dag_id="api2csv",
    schedule_interval="@daily",
    catchup= False,
    start_date= pendulum.datetime(2023,9,30),
) as dag:
    task_is_api_alive = HttpSensor(
        task_id = "check_api_status",
        http_conn_id="api_posts",
        endpoint="posts/",
    )
    
    get_posts = SimpleHttpOperator(
        task_id = "get_posts_from_api",
        http_conn_id= "api_posts",
        endpoint="posts/",
        method="GET",
        response_filter= lambda response: json.loads(response.text),
        log_response=True,
    )
    
    def save_json(ti):
        posts = ti.xcom_pull(task_ids=["get_posts_from_api"])
        # print("printing response content{}".format(posts[0]))
        with open("/mnt/c/Users/premv/airflow/responses/response.json","w") as f:
            json.dump(posts[0],f,indent=5)
            
    save = PythonOperator(
        task_id = "save_json",
        python_callable=save_json,
    )
    
    task_is_api_alive >> get_posts >> save