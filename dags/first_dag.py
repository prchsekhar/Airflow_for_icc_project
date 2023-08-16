from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'retries': 1,
   'retry_delay':timedelta(minutes=10)
}

with DAG( default_args=default_args,
          dag_id='hello_world', 
          description='Hello World DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2023, 4, 6), 
          end_date=None,
          catchup=False)as dag:

    hello_operator = PythonOperator(
        task_id='hello_task', 
        python_callable=print_hello
        )

hello_operator