from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 5),  # Adjusted start_date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rundatabricksnotebook0908',
    default_args=default_args,
    description='Databricks',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

new_cluster = {
    'spark_version': '12.2.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
    'num_workers': 1,  # Specify the number of workers
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
        'notebook_path': '/Workspace/Users/as23aeb@herts.ac.uk/Housepricepredicition_notebook_2'
    }
}

notebooktask = DatabricksSubmitRunOperator(
    task_id="airflowmodelrun",
    databricks_conn_id="databricks_default",
    dag=dag,
    json=notebook_task_params,
    timeout_seconds=3600
)

# Define the task dependency correctly
start_task >> notebooktask >> end_task
