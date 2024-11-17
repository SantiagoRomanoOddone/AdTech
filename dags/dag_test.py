import datetime
import numpy as np
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup




with DAG(
    dag_id='test',
    schedule=None, # None means do not schedule
    start_date=datetime.datetime(2024, 11, 5), # Start date of the DAG
    catchup=False, # If start_date is in the past, it will run all the tasks between start_date and now. Take care of this parameter
    ) as dag:
        sleep = BashOperator( # BashOperator is a type of operator that executes a bash command
            task_id='sleep', # Name of the task
            bash_command='sleep 3', # Execute a bash command
        )

        def sample_normal(mean, std):
            return np.random.normal(loc=mean, scale=std)
        
        random_height = PythonOperator( # PythonOperator is a type of operator that executes a Python function
            task_id='height', # The name of the task.
            python_callable=sample_normal, # The function to execute without the parameters
            op_kwargs = {"mean" : 170, "std": 15}, # parameters to pass to the function
        )
        
        # Create 2 dummy tasks to define dependencies
        first = BashOperator(task_id='first', bash_command='sleep 3 && echo First')
        last = BashOperator(task_id='last', bash_command='sleep 3 && echo Last')

        # Using the >> operator to define dependencies, a >> b means b runs after a finishes
        first >> sleep
        first >> random_height

        # We can group multiple tasks when defining precedences
        [sleep, random_height] >> last

        # Any task defined within the 'with' clause will be part of the group
        with TaskGroup(group_id="post_process") as post_process_group:
            for i in range(5):
                EmptyOperator(task_id=f"dummy_task_{i}")

        # We can define dependencies at the task group level
        last >> post_process_group