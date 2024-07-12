from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 13),  # Adjusted start_date
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

# Define function-based poke function for the CustomSensor
def poke_fn(task_instance, **kwargs):
    external_dag_id = kwargs.get('external_dag_id')
    external_task_id = kwargs.get('external_task_id')
    execution_delta = kwargs.get('execution_delta')

    execution_date = kwargs['execution_date']
    external_dag_run = task_instance.get_dagrun(
        external_dag_id,
        execution_date - execution_delta,
        execution_date + execution_delta
    )
    if not external_dag_run:
        return False
    external_task_instance = external_dag_run.get_task_instance(external_task_id)
    return external_task_instance.is_complete()

# Define function-based CustomSensor to wait for the completion of the prerequisite DAG
def external_task_completion_sensor(task_id, external_dag_id, external_task_id, execution_delta, timeout, poke_interval, dag):
    return BaseSensorOperator(
        task_id=task_id,
        poke_interval=poke_interval,
        timeout=timeout,
        poke_fn=lambda task_instance, **kwargs: poke_fn(task_instance, external_dag_id=external_dag_id, external_task_id=external_task_id, execution_delta=execution_delta),
        dag=dag,
    )

sensor_task = external_task_completion_sensor(
    task_id='wait_for_completion_of_other_dag',
    external_dag_id='hello_world',  # Replace with the ID of the prerequisite DAG
    external_task_id='say_hello',  # Replace with the task ID in the other DAG to wait for
    execution_delta=timedelta(seconds=20),  # Wait for 20 seconds after completion
    timeout=3600,  # Timeout after 1 hour if the other DAG doesn't complete
    poke_interval=30,  # Check every 30 seconds for task completion
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
