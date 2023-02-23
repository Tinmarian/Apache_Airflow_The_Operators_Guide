"""
### Uso de los helpers: "cross_downstream" y "chain".

Estos helpers nos van a ayudar a definir las dependencias
"""


from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
# from airflow.utils.helpers import cross_downstream
from airflow.exceptions import AirflowTaskTimeout
from airflow.models.baseoperator import cross_downstream, chain

def failure_callback(context):
        if (isinstance(context['exception'], AirflowTaskTimeout)):
            print('The task timed out')
        else:
             print('Other Error')




with DAG(
            '7_cross_chain_helpers',
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
                                execution_timeout=timedelta(seconds=(11)),
                                on_failure_callback=failure_callback
                            )

    extract_b = BashOperator(
                                owner='Armando',
                                task_id='extract_b',
                                bash_command="echo 'extract_b' && sleep 5",
                                execution_timeout=timedelta(seconds=(6)),
                                on_failure_callback=failure_callback
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
    
    chain([process_a,process_b,process_c],[clean_a,clean_b,clean_c])
    # process_a >> clean_a
    # process_b >> clean_b
    # process_c >> clean_c
    chain([process_a,process_b,process_c], store)

    # process_a >> clean_a
    # process_b >> clean_b
    # process_c >> clean_c

    # [process_a,process_b,process_c] >> store
