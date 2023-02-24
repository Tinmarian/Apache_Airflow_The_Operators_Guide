"""
### Uso del PostgresOperator

Este operador se utiliza para interactuar con una base de datos 
postgresql mediante lenguaje SQL...

Para finalizar, vamos a crear una clase nueva (un Operador) para
generar parámetros dinámicos dentro del nuevo Operador.
"""

from airflow import DAG
from datetime import datetime

from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

dag_owner = 'Tinmar'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 0
        }

class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')

def _print_sql(ti):
    sql = ti.xcom_pull(task_ids='postgres_select', key='return_value')
    print(sql)

@task(task_id='prueba')
def my_task():
    return 'tweets.csv'

with DAG(
        dag_id='9.3_postgres_dag',
        default_args=default_args,
        description='',
        start_date=datetime(2023,2,23),
        schedule_interval=None,
        catchup=False,
        tags=['Curso4', 'The Operators Guide']
    )as dag:

    dag.doc_md = __doc__

    start = DummyOperator(task_id='start')

    postgres_create = PostgresOperator(
                                        task_id='postgres_create',
                                        sql="sql/create.sql"   
                                    )

    postgres_insert = CustomPostgresOperator(
                                    task_id='postgres_insert',
                                    sql="sql/insert_3.sql",
                                    parameters={
                                        "filename":'{{ ti.xcom_pull(task_ids="prueba")[0] }}'
                                    }
                                )
    
    postgres_select = PostgresOperator(
                                        task_id='postgres_select',
                                        sql="SELECT * FROM my_table"   
                                    )
    
    print_sql = PythonOperator(
                                task_id='print_sql',
                                python_callable=_print_sql 
                            )

    end = DummyOperator(task_id='end')

    start >> postgres_create >> my_task() >> postgres_insert >> postgres_select >> print_sql >> end
