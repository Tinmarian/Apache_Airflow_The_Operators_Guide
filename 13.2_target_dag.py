"""
### Uso del TriggerDagRunOperator

Este es el DAG externo que se va a ejecutar mediante el operador 
TriggerDagRunOperator.
"""

from airflow import DAG
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner":"Tinmar",
    "start_date":datetime(2023,2,27)
}

with DAG(
            '13.2_target_dag',
            catchup=False,
            schedule=None,
            default_args=default_args,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    start = DummyOperator(task_id='start')

    process = BashOperator(
                            task_id='process',
                            bash_command="echo '{{ dag_run.conf['path'] }}' && sleep 15"   
                        )

    end = DummyOperator(task_id='end')

    start >> process >> end