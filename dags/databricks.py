from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
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

dag = DAG(
    'rundatabricksnotebook0908',  # Removed trailing space
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

# Define ExternalTaskSensor to wait for the completion of the prerequisite DAG
sensor_task = BaseSensorOperator(
    task_id='wait_for_completion_of_other_dag',
    external_dag_id='hello_world',  # Replace with the ID of the prerequisite DAG
    external_task_id='say_hello',  # Replace with the task ID in the other DAG to wait for
    execution_delta=timedelta(seconds=20),  # Wait for 20 seconds after completion
    timeout=3600,  # Timeout after 1 hour if the other DAG doesn't complete
    dag=dag,
)

new_cluster = {
    'spark_version': '12.2.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
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
        'notebook_path': '/Workspace/Users/karimullas.de03@praxis.ac.in/Housepricepredicitionnotebook2'
    }
}

notebooktask = DatabricksSubmitRunOperator(
    task_id="airflowmodelrun",
    databricks_conn_id="databricks_default",
    dag=dag,
    json=notebook_task_params,
    timeout_seconds=3600
)

# Define the task dependency
sensor_task >> start_task >> notebooktask >> end_task
