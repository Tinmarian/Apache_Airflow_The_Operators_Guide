from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

def a():
    print('a')

def b():
    print('b')

def c():
    print('c')

def subdag_(parent_dag_id, subdag_id, default_args):
    with DAG(
                f"{parent_dag_id}.{subdag_id}",
                default_args=default_args,
                schedule=None
            ) as dag:
    
        start = DummyOperator(task_id='start')

        training_a = PythonOperator(
                                    task_id='training_a',
                                    python_callable=a
                                )
    
        training_b = PythonOperator(
                                    task_id='training_b',
                                    python_callable=b
                                )
        
        training_c = PythonOperator(
                                    task_id='training_c',
                                    python_callable=c
                                )
        
        end = DummyOperator(task_id='end', trigger_rule='all_done')

        start >> [training_a,training_b,training_c] >> end
        
    return dag