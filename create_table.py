from airflow.decorators import dag,task
import pendulum
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

@dag(
    dag_id = "create_mssql_table",
    schedule = "@daily",
    start_date = pendulum.datetime(2023,9,30),
    catchup = False,
    tags = ["my-dags"],
    
)
def dag_task():
    @task
    def create_tbl():
        create_table = MsSqlOperator(
            task_id = "create_table",
            mssql_conn_id="mssql_default",
            sql = """
            create table if not exist created_from_airflow(colA int)
            """
            )
    create_tbl()
dag_task()