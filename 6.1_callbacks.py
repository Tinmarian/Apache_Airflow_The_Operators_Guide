"""
### Uso de los argumentos "execution_timeout", "on_success_callback"
y "on_failure_callback".

El primero de ellos nos va a permitir esperar una determinada 
cantidad de tiempo para que una tarea finalice, una vez que haya 
pasado el tiempo determinado, la tarea se definirá como fallida.

El segundo y el tercero son argumentos que nos van a permitir 
generar un callback a una función definida previamente. Esto quiere
decir que podemos hacer un callback si la tarea es fallida o tuvo
éxito.


"""


from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.helpers import cross_downstream


def _on_success_callback(context):
    print(context)

def _on_failure_callback(context):
    print(context)

with DAG(
            '6.1_callbacks',
            start_date=datetime(2023,2,21),
            default_args={"owner":"Andrade"},
            schedule='@daily',
            catchup=False,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    extract_a = BashOperator(
                                owner='Tinmar', 
                                task_id='extract_a', 
                                bash_command="echo 'extract_a' && sleep 10",
                                execution_timeout=timedelta(seconds=(10)),
                                on_success_callback=_on_success_callback,
                                on_failure_callback=_on_failure_callback
                            )

    extract_b = BashOperator(
                                owner='Armando',
                                task_id='extract_b',
                                bash_command="echo 'extract_b' && sleep 5"
                            )

    process_a = BashOperator(
                                owner='Armando',
                                task_id='process_a',
                                bash_command="echo 'process_a' && sleep 5",
                                pool='sequential_pool'
                            )
    
    clean_a = BashOperator(
                            task_id='clean_a',
                            bash_command="echo 'clean_a' && sleep 5",
                            trigger_rule='one_failed' 
                        )

    process_b = BashOperator(
                                owner='Armando',
                                task_id='process_b',
                                bash_command="echo 'process_b' && sleep 5",
                                pool='sequential_pool'
                            )
    
    clean_b = BashOperator(
                            task_id='clean_b',
                            bash_command="echo 'clean_b' && sleep 5",
                            trigger_rule='one_failed'    
                        )

    process_c = BashOperator(
                                owner='Armando',
                                task_id='process_c',
                                bash_command="echo 'process_c' && exit 1",
                                pool='sequential_pool'
                            )
    
    clean_c = BashOperator(
                            task_id='clean_c',
                            bash_command="echo 'clean_c' && sleep 5",
                            trigger_rule='one_failed'    
                        )


    store = BashOperator(
                            task_id='store', 
                            bash_command="sleep 10 && exit 0",
                            retries=3,
                            retry_delay=timedelta(seconds=10),
                            retry_exponential_backoff=True,
                            trigger_rule='all_done'
                        )

    # [extract_a,extract_b] >> [process_a,process_b,process_c] >> task_b  # Este arreglo de dependencias no funciona
    cross_downstream([extract_a,extract_b], [process_a,process_b,process_c]) 
    
    process_a >> clean_a
    process_b >> clean_b
    process_c >> clean_c

    [clean_c,clean_a,clean_b] >> store

    # process_a >> clean_a
    # process_b >> clean_b
    # process_c >> clean_c

    # [process_a,process_b,process_c] >> store
