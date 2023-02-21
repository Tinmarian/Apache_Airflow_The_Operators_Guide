"""
### Uso del argumento "pool".

Antes de utilizar el argumento "pool" será necesario crear una pool
dentro de la UI de Airflow. De esta manera podremos elegir entre
nuestras pools existentes.

Por defecto, todas las tasks se definen en la "default_pool", la
cual contiene 128 nodos (workers) que pueden trabajar en paralelo, 
dependiendo de los recursos disponibles
"""


from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.helpers import cross_downstream

with DAG(
            '4_pools',
            start_date=datetime(2023,2,21),
            default_args={"owner":"Andrade"},
            schedule='@daily',
            catchup=False,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    extract_a = BashOperator(owner='Tinmar', task_id='extract_a', bash_command="echo 'extract_a' && sleep 5")

    extract_b = BashOperator(owner='Armando', task_id='extract_b', bash_command="echo 'extract_b' && sleep 5")

    process_a = BashOperator(owner='Armando', task_id='process_a', bash_command="echo 'process_a' && sleep 5", pool='sequential_pool')

    process_b = BashOperator(owner='Armando', task_id='process_b', bash_command="echo 'process_b' && sleep 5", pool='sequential_pool')

    process_c = BashOperator(owner='Armando', task_id='process_c', bash_command="echo 'process_c' && sleep 5", pool='sequential_pool')


    store = BashOperator(
                            task_id='store', 
                            bash_command="sleep 10 && exit 0",
                            retries=3,
                            retry_delay=timedelta(seconds=10),
                            retry_exponential_backoff=True
                        )

    # [extract_a,extract_b] >> [process_a,process_b,process_c] >> task_b  # Este arreglo de dependencias no funciona
    cross_downstream([extract_a,extract_b], [process_a,process_b,process_c]) 
    [process_a,process_b,process_c] >> store