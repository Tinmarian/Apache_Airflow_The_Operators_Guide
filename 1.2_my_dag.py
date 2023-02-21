from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator

with DAG(
            '1.2_my_dag',
            start_date=datetime(2023,2,21),
            default_args={"owner":"Tinmar"},
            schedule='@daily',
            catchup=False,
            tags=['Curso 4', 'The Operators Guide']
        ):
    
    task_a = BashOperator(task_id='task_a', bash_command="echo 'task_a'")

    task_c = BashOperator(task_id='task_c', bash_command="echo 'task_c'")

    task_b = BashOperator(task_id='task_b', bash_command="echo 'task_c'")

    task_a >> task_c >> task_b