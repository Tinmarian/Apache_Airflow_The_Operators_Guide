"""
### Uso de ShortCircuitOperator

Este operador nos permite tener tareas con diferente schedule en un 
mismo DAG.

El operador funciona mediante sentencias True o False, en caso de 
True, las tareas que siguen al operador shortcircuit se ejecutan, 
en caso de False, no se ejecutan.

Se define una función en python con el argumento "execution_date"
y se retorna el siguiente valor:

    * return execution_Date.weekday() == 0

En donde el número representa el día de la semana, siendo el 0 el 
número represantativo del Lunes, y así sucesivamente.
"""

from airflow import DAG 
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator 

default_args={
    "owner":"Tinmar",
    "start_date":datetime(2023,2,28)
}

def short_(execution_date):
    return execution_date.weekday() == 1

with DAG(
            '15_short_circuit',
            catchup=False,
            default_args=default_args,
            schedule=None,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    task_a = DummyOperator(task_id='task_a')
    task_b = DummyOperator(task_id='task_b')
    task_c = DummyOperator(task_id='task_c')

    short = ShortCircuitOperator(
                                    task_id='short',
                                    python_callable=short_   
                                )

    task_d = DummyOperator(task_id='task_d')

    task_a >> task_b >> task_c >> short >> task_d