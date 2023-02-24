"""
### Uso del BranchPythonOperator

El uso de este operador nos va a permitir elegir un camino u otro 
de ejecuciones.

Como veremos, no solo podemos elegir una sola task a ejecutar 
depués del BranchPythonOperator, sino que podemos pasar una lista
de tasks a ejecutar dentro del BranchPythonOperator.
"""


from airflow import DAG
from datetime import datetime

from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import BranchPythonOperator

def _branch():
    accuracy = 0.16
    if accuracy>0.15:
        return ['accurate', 'top']
    return 'inaccurate'

with DAG(
            '10.1_branch_operators',
            catchup=False,
            start_date=datetime(2023,2,24),
            schedule=None,
            default_args={"owner":"Tinmar"},
            tags=['Curso 4', 'The Operators Guide']  
        ) as dag:
    
    dag.doc_md = __doc__

    training = DummyOperator(
        task_id='training'
    )
    
    branch = BranchPythonOperator(
                                    task_id='branch',
                                    python_callable=_branch   
                                )
    
    accurate = DummyOperator(
        task_id="accurate",
    )

    top = DummyOperator(
        task_id="top",
    )

    inaccurate = DummyOperator(
        task_id="inaccurate",
    )

    publish = DummyOperator(
        task_id='publish',
        trigger_rule='one_success'
    )

    training >> branch >> [top,accurate,inaccurate] >> publish