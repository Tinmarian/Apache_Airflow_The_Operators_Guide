"""
### Uso de TaskGroup.

TaskGroup es una alternativa al operador SubDagOperator. De esta 
forma es más sencillo agrupar tareas.

Se pueden agrupar dentro del mismo DAG o generarando un nuevo 
archivo .py en donde se defina una función que contenga el 
TaskGroup deseado.

Aquí vamos a generar un nuevo archivo .py para definir el TaskGroup.
"""


from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime
from Apache_Airflow_The_Operators_Guide._12_task_group import task_group

# /c/Airflow/dags/Apache_Airflow-The_Operators_Guide/_11_sub_dag.py

default_args={
    "owner":"Tinmar",
    "start_date":datetime(2023,2,27)
}

with DAG(
            '12_task_group',
            catchup=False,
            default_args=default_args,
            schedule=None,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__
    
    start = BashOperator(
                            task_id='start',
                            bash_command="echo 'start'"   
                        )
    
    group = task_group()
    
    end = BashOperator(
                        task_id='end',
                        bash_command="echo 'end'"   
                    )
    
    start >> group >> end
