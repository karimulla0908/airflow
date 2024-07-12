from airflow import DAG
from airflow.operators.dummy import DummyOperator
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
        'notebook_path': 'Users/karimullas.de03@praxis.ac.in/Housepricepredicitionnotebook2'
    }
}

notebooktask = DatabricksSubmitRunOperator(
    task_id="airflowmodelrun",
    databricks_conn_id="databricks_default",
    dag=dag,
    json=notebook_task_params
)

start_task >> notebooktask >> end_task
