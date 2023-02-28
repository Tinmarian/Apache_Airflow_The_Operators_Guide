"""
### Uso del SubDagOperator.

Aquí utilizamos este operador para poder implementar un subdag
dentro de un DAG.

Sin embargo, al parecer en la versión 2.4.0 de Airflow es necesario
utilizar Celery, Kubernetes o CeleryKubernetes Executors, de otra 
manera, el subdag no ejecutará ninguno de los operadores contenidos 
en las tareas, con excepción del DummyOperator.
"""


from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from datetime import datetime
from Apache_Airflow_The_Operators_Guide._11_sub_dag import subdag_

# /c/Airflow/dags/Apache_Airflow-The_Operators_Guide/_11_sub_dag.py

default_args={
    "owner":"Tinmar",
    "start_date":datetime(2023,2,27)
}

with DAG(
            '11_parent_dag',
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
    
    group = SubDagOperator(
                            task_id='group',
                            subdag=subdag_(dag.dag_id,'group',default_args)
                        )
    
    end = BashOperator(
                        task_id='end',
                        bash_command="echo 'end'"   
                    )
    
    start >> group >> end
