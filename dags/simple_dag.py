from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task
hello_task = BashOperator(
    task_id='say_hello',
    bash_command='echo "Hello, World!"',
    dag=dag,
)

# Define the task dependencies
hello_task
