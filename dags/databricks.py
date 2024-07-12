from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2024, 7, 13),  # Adjust start date as per your requirement
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
with DAG(
    'run_databricks_notebook',
    default_args=default_args,
    description='Run Databricks Notebook DAG',
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
) as dag:

    # Define the task to run the Databricks notebook
    run_databricks_notebook = DatabricksSubmitRunOperator(
        task_id='run_databricks_notebook_task',
        databricks_conn_id='databricks_default',  # Connection ID configured in Airflow
        new_cluster={
            'spark_version': '7.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 1
        },
        notebook_task={
            'notebook_path': '/Workspace/Users/karimullas.de03@praxis.ac.in/Housepricepredicition_notebook_2',  # Replace with your notebook path
        },
        timeout_seconds=3600,  # Adjust timeout as per your notebook execution time
    )

    # If there are more tasks, set task dependencies here
    # run_databricks_notebook >> another_task

    run_databricks_notebook  # This line sets the task in the DAG
