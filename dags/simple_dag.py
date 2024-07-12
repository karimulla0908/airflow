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

class ExternalTaskCompletionSensor(BaseSensorOperator):
    def __init__(self, external_dag_id, external_task_id, execution_delta, *args, **kwargs):
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.execution_delta = execution_delta
        super().__init__(*args, **kwargs)

    def poke(self, context):
        execution_date = context['execution_date']
        external_dag_run = self.get_dagrun(
            self.external_dag_id,
            execution_date - self.execution_delta,
            execution_date + self.execution_delta
        )
        if not external_dag_run:
            return False
        external_task_instance = external_dag_run.get_task_instance(self.external_task_id)
        return external_task_instance.is_complete()

sensor_task = ExternalTaskCompletionSensor(
    task_id='wait_for_completion_of_other_dag',
    external_dag_id='hello_world',  # Replace with the ID of the prerequisite DAG
    external_task_id='say_hello',  # Replace with the task ID in the other DAG to wait for
    execution_delta=timedelta(seconds=20),  # Wait for 20 seconds after completion
    timeout=3600,  # Timeout after 1 hour if the other DAG doesn't complete
    poke_interval=30,  # Check every 30 seconds for task completion
    dag=dag,
)

# Define the task
hello_task = BashOperator(
    task_id='say_hello',
    bash_command='echo "Hello, World!"',
    dag=dag,
)

# Define the task dependencies
hello_task
