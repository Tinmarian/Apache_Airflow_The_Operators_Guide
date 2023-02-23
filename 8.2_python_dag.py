"""
### Decorador "task" para sustituir el PythonOperator

Aquí vamos a utilizar el decorador "task" en las funciones python
con el objetivo de reducir las líneas de código en nuestro DAG.
"""

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime

@task(task_id='python_a')
def _python_a():
    print('Hello python')

@task(task_id='python_b')
def _python_b(path, filename):
    print(f'{path}/{filename}')

@task(task_id='python_c')
def _python_c(**context):
    print(f"{context['ds']}")

@task(task_id='python_d')
def _python_d(python_dag):
    print(f"{python_dag['path']}/{python_dag['filename']}")

with DAG(
            '8.2_python_dag',
            catchup=False,
            start_date=datetime(2023,2,23),
            schedule=None,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__
    
    _python_a()
    _python_b('/c/user/tinma/', 'python.py')
    _python_c()
    _python_d(Variable.get('python_dag',deserialize_json=True))