"""
### Funcionamiento del PythonOperator

En este dag vamos a dar algunos ejemplos de cómo se puede utilizar 
el PythonOperator dentro de nuestros DAG's.
"""


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

def _python_a():
    print('Hello python')

def _python_b(path, filename):
    print(f'{path}/{filename}')

def _python_c(path, filename, **context):
    print(f"{path}/{filename} - {context['ds']}")

with DAG(
            '8.1_python_dag',
            catchup=False,
            start_date=datetime(2023,2,23),
            schedule=None,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    python_a = PythonOperator(
                                task_id='python_a',
                                python_callable=_python_a   
                            )
    
    python_b = PythonOperator(
                                task_id='python_b',
                                python_callable=_python_b,
                                op_args=['/c/users/tinma','python.py']   
                            )
    
    python_c = PythonOperator(
                                task_id='python_c',
                                python_callable=_python_c,
                                op_kwargs={'path': '/c/users/tinma','filename':'python.py'}   
                            )
    
    python_d = PythonOperator(
                                task_id='python_d',
                                python_callable=_python_c,
                                op_kwargs=Variable.get('python_dag', deserialize_json=True)   
                            )