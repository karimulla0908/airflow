from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,8,5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'datascraping',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task
data_scraper = BashOperator(
    task_id='data',
    bash_command='echo "Hello, World!"',
    dag=dag,
)

start_task = DummyOperator(task_id='start_task', dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

databricks_trigger = TriggerDagRunOperator(
    task_id='databricks_task',
    trigger_dag_id='rundatabricksnotebook0908',  # The DAG ID of the DAG to trigger
    dag=dag,
)

# Define the task dependencies
start_task >> data_scraper >> databricks_trigger >> end_task
