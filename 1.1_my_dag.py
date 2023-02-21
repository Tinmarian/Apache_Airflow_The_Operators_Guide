from airflow import DAG
from datetime import datetime

from airflow.operators.dummy import DummyOperator

with DAG(
            '1.1_my_dag',
            start_date=datetime(2023,2,21),
            default_args={"owner":"Tinmar"},
            schedule='@daily',
            catchup=False,
            tags=['Curso 4', 'The Operators Guide']
        ):
    
    task_a = DummyOperator(task_id='task_a')

    task_c = DummyOperator(task_id='task_c')

    task_b = DummyOperator(task_id='task_b')

    task_a >> task_c >> task_b