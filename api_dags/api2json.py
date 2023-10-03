import requests
import json

from airflow.decorators import dag,task
import pendulum

@dag(
    dag_id="api2json_taskflow_api",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023,9,30),
    catchup=False,
    tags=["my-dags","taskflow_api"],
)
def taskflow_api():
    """this is dag created by prem to build pipeline using task flow api
    """
    @task
    def get_posts():
        resp = requests.get("https://gorest.co.in/public/v2/posts/")
        if resp.status_code == 200:
            # print(resp.content)
            print("response without error")
            return json.loads(resp.text)
        else:
            print("unable to get response from gorest API")

    @task
    def save_response(res):
        with open("/mnt/c/Users/premv/airflow/responses/taskflow_api_res.json","w") as f:
            json.dump(res,f,indent=5)

    resp = get_posts()
    save_response(resp)
taskflow_api()