"""
### Uso del argumento "trigger_rule".

Este argumento nos permite darle instrucciones más específicas a 
cada tarea sobre cuál es el tipo de trigger asignado.

Cada tipo de trigger hace referencia a un hecho, y con base en
el cumplimiento, o no, de ese hecho, se ejecuta la tarea, se salta 
la tarea o se ejecuta como fallida.

Los diferentes valores para este parámetro son los siguientes:

    * all_success
    * all_failed
    * all_done
    * one_failed
    * one_success
    * none_failed
    * none_failes_or_skipped
    * none_skipped
"""


from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.helpers import cross_downstream

with DAG(
            '5_trigger_rules',
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
                                bash_command="echo 'extract_a' && sleep 5"
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
