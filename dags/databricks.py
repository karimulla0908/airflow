from airflow import DAG
from airflow.operators.dummy import DummyOperator
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
    'retry_delay': timedelta(minutes=10),
}

# Define your DAG
with DAG(
    'run_databricks_notebook',
    default_args=default_args,
    description='Run Databricks Notebook DAG',
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
) as dag:

    start_task = DummyOperator(
        task_id='start_task',
        dag=dag,
    )

    end_task = DummyOperator(
        task_id='end_task',
        dag=dag,
    )

    new_cluster = {
        'spark_version': '7.6.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'num_workers': 0,
        'spark_conf': {
            'spark.databricks.cluster.profile': 'singleNode',
            'spark.master': 'local[*]'
        },
        'custom_tags': {
            'TeamName': 'MLOPS Project'
        }
    }

    notebook_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/Users/karimullas.de03@praxis.ac.in/Housepricepredicition_notebook_2'
        }
    }

    notebook_task = DatabricksSubmitRunOperator(
        task_id="airflow_model_run",
        databricks_conn_id="databricks_default",
        dag=dag,
        json=notebook_task_params
    )

    start_task >> notebook_task >> end_task
