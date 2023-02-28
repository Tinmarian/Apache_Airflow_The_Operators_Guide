from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

def a():
    print('a')

def b():
    print('b')

def c():
    print('c')

def task_group():
    with TaskGroup('training_tasks') as training_tasks:
    

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
        
        

        with TaskGroup('publish_tasks') as publish_tasks:
        
            publish_a = PythonOperator(
                                        task_id='training_a',
                                        python_callable=a
                                    )

            publish_b = PythonOperator(
                                        task_id='training_b',
                                        python_callable=b
                                    )
            
            publish_c = PythonOperator(
                                        task_id='training_c',
                                        python_callable=c
                                    )
            
        [training_a,training_b,training_c] >> publish_tasks

    return training_tasks