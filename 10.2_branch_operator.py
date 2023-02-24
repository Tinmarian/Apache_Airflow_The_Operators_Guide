"""
### Uso del BranchPythonOperator

El uso de este operador nos va a permitir elegir un camino u otro 
de ejecuciones.

Ahora veamos cómo es que podemos ejecutar tareas con base en los 
días del calendario que nosotros necesitemos.

Vamos a ver primero con una lista sencilla que contenga los días 
que no deseamos ejecutar.

Después, vamos a generar un archivo con los días que no queremos
ejecutar las tareas y tomaremos los días de dicho archivo.
"""


from airflow import DAG
from datetime import datetime
import yaml

from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import BranchPythonOperator

def _branch(ds):
    days_off = ['2023-02-25', '2023-02-23', '2023-02-22', '2023-02-21', '2023-02-20']
    if ds not in days_off:
        return 'process_1'
    return 'stop_1'

def _branch_2(ds):
    with open('/c/Airflow/dags/Apache_Airflow-The_Operators_Guide/files/days_off.yml') as f:
        days_off = set(yaml.load(f,Loader=yaml.FullLoader))
        if ds not in days_off:
            return 'process_2'
        return 'stop_2'

with DAG(
            '10.2_branch_operators',
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
    
    branch_1 = BranchPythonOperator(
                                    task_id='branch_1',
                                    python_callable=_branch   
                                )
    
    process_1 = DummyOperator(
        task_id='process_1'
    )

    stop_1 = DummyOperator(
        task_id='stop_1'
    )

    branch_2 = BranchPythonOperator(
                                    task_id='branch_2',
                                    python_callable=_branch_2,
                                    trigger_rule='one_success'
                                )
    
    stop_2 = DummyOperator(
        task_id='stop_2'
    )

    process_2 = DummyOperator(
        task_id='process_2'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    start >> branch_1 >> [process_1,stop_1] >> branch_2 >> [process_2,stop_2] >> end