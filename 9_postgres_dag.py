"""
### Uso del PostgresOperator

Este operador se utiliza para interactuar con una base de datos 
postgresql mediante lenguaje SQL.

Primero creamos una tabla dentro de la base de datos y después
insertamos datos dentro de esa tabla. 

Por último, generamos una 
consulta y mediante el PythonOperator y un XCom realizamos el 
print del resultado de la consulta.

También podemos ejecutar una lista de instrucciones sql, dentro de 
la cual puede incluso ejecutarse un archivo sql mediante el path
explícito en la lista.
"""

from airflow import DAG
from datetime import datetime, timedelta 

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

dag_owner = 'Tinmar'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 0
        }

def _print_sql(ti):
    sql = ti.xcom_pull(task_ids='postgres_insert', key='return_value')
    print(sql)

with DAG(
        dag_id='9_postgres_dag',
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

    postgres_insert = PostgresOperator(
                                    task_id='postgres_insert',
                                    sql=["sql/insert.sql", "SELECT * FROM my_table"]
                                )
    
    # postgres_select = PostgresOperator(
    #                                     task_id='postgres_select',
    #                                     sql="SELECT * FROM my_table"   
    #                                 )
    
    print_sql = PythonOperator(
                                task_id='print_sql',
                                python_callable=_print_sql 
                            )

    end = DummyOperator(task_id='end')

    start >> postgres_create >> postgres_insert >> print_sql >> end
