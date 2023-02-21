"""
### Uso del argumento "owner".

El argumento owner tiene un uso fundamental: referir a quien creo 
el DAG. Esto puede ser de utilidad si son varias personas las 
encargadas del DAG.

Una parte fundamental del argumento es que se aplica
directamente a las tasks del DAG.

Se puede definir en "default_args" para definir un solo "owner" para
todas las tasks o, se puede definir un "owner" específico para
cada task. 
"""


from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator

with DAG(
            '2_owner',
            start_date=datetime(2023,2,21),
            default_args={"owner":"Andrade"},
            schedule='@daily',
            catchup=False,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    task_a = BashOperator(owner='Tinmar', task_id='task_a', bash_command="echo 'task_a'")

    task_c = BashOperator(owner='Armando', task_id='task_c', bash_command="echo 'task_c'")

    task_b = BashOperator(task_id='task_b', bash_command="echo 'task_c'")

    task_a >> task_c >> task_b