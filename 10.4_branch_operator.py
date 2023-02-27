"""
### Uso del BranchDateTimeOperator.

Este operador regresa True o False, dependiendo de su hora de 
ejecución. De esta manera, si su datetime de ejecución está dentro 
de los datetime definidos por los parámetros:

    * target_lower
    * target_upper

Entonces, los parámetros:

    * follow_task_ids_if_true
    * follow_task_ids_if_false

Definen qué tareas ejecutar en caso de ser False o True el resultado
del Operador.

Se devuelve True si el Operador se ejecuto dentro de los datetime 
definidos en el operador. Y False si no se ejecutó dentro de dichos
datetime.
"""


from airflow import DAG
from datetime import datetime, time

from airflow.operators.dummy import DummyOperator 
from airflow.operators.datetime import BranchDateTimeOperator

with DAG(
            '10.4_branch_operators',
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

    time_ = BranchDateTimeOperator(
                                    task_id='time',
                                    follow_task_ids_if_true='move_0',
                                    follow_task_ids_if_false='end',
                                    # target_lower=datetime(2023,2,25,8,30,0),
                                    # target_upper=datetime(2023,2,25,9,0,0),
                                    # target_lower=time(15,0,0),  # Esto es la feha de hoy con solo
                                    # target_upper=time(16,0,0),  # la hora del día en UTC
                                    target_lower=datetime(2023,2,27,18,10,0),  # Esto es la feha de hoy con la hora
                                    target_upper=datetime(2023,2,27,18,50,0),  # del día en local timezone
                                    use_task_execution_date=True
                                    
                                )
    
    move_0 = DummyOperator(task_id='move_0')

    move_1 = DummyOperator(task_id='move_1')

    move_2 = DummyOperator(task_id='move_2')


    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )

    start >> time_ >> move_0 >> [move_1,move_2] 
    move_0 >> end