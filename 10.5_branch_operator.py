"""
### Uso del BranchDayOfWeekOperator.

Este operador regresa True o False, dependiendo del día de 
ejecución actual, y no dependiente del día de la ejecución del 
DAG Run. 
Los días en los cuáles se ejecutará la tarea están definidos por 
el parámetro:

    * week_day

El cual puede recibir un solo día o una lista de días mediante el
comando WeekDay, el cual se importa desde:

    * airflow.operators.weekday

Entonces, los parámetros:

    * follow_task_ids_if_true
    * follow_task_ids_if_false

Definen qué tareas ejecutar en caso de ser False o True el resultado
del Operador.

Se devuelve True si el día actual de ejecución es un día dentro de
la lista de días pasados al parámetro "week_day". Y False, si el día
de ejecución del DAG no está en la lista de días del parámetro
"week_day".

El parámetro:

    * use_task_execution_day

Nos va a permitir que el operador sea sensible no solo al día de 
ejecución actual, sino también a DAG Runs pasadas.

Si este parámetro se queda en False, será el current day el que se
tomará en cuenta. En caso de ser True, será el execution date el que 
se tomará en cuenta.
"""


from airflow import DAG
from datetime import datetime, time

from airflow.operators.dummy import DummyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.weekday import WeekDay 

with DAG(
            '10.5_branch_operators',
            catchup=False,
            start_date=datetime(2023,2,24),
            schedule=None,
            default_args={"owner":"Tinmar"},
            tags=['Curso 4', 'The Operators Guide']  
        ) as dag:
    
    dag.doc_md = __doc__

    start = DummyOperator(
        task_id='start'
    )

    branch = BranchDayOfWeekOperator(
                                        task_id='branch',
                                        follow_task_ids_if_true='move_0',
                                        follow_task_ids_if_false='end',
                                        week_day=[WeekDay.TUESDAY],  #, WeekDay.MONDAY]
                                        use_task_execution_day=True
                                    )
    
    move_0 = DummyOperator(task_id='move_0')

    move_1 = DummyOperator(task_id='move_1')

    move_2 = DummyOperator(task_id='move_2')


    end = DummyOperator(
        task_id='end'
    )

    start >> branch >> [move_0,end]
    move_0 >> [move_1,move_2]