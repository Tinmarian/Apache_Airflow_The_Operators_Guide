"""
### Uso del BranchSQLOperator.

En este DAG vamos a comenzar a utilizar el BranchSQLOperator en
nuestros DAG's.

Este operador nos va a ayudar a revisar datos dentro de una 
base de datos y decidir qué tarea ejecutar con base en los datos
obtenidos en la revisión
"""


from airflow import DAG
from datetime import datetime
import yaml

from airflow.operators.dummy import DummyOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.python import PythonOperator

with DAG(
            '10.3_branch_operators',
            catchup=False,
            start_date=datetime(2023,2,24),
            schedule=None,
            default_args={"owner":"Tinmar"},
            tags=['Curso 4', 'The Operators Guide']  
        ) as dag:
    
    dag.doc_md = __doc__

    start = DummyOperator(
        task_id='start'
    )
    
    create = PostgresOperator(
                                task_id='create',
                                sql='sql/create_partners.sql'   
                            )
    
    insert = PostgresOperator(
                                task_id='insert',
                                sql='sql/insert_partners.sql'  
                            )
    
    sql = BranchSQLOperator(
                                task_id='sql',
                                sql='sql/branch.sql',
                                follow_task_ids_if_true=['process'],
                                follow_task_ids_if_false=['notif_email', 'notif_slack'],
                                conn_id='postgres',
                                database='airflow',
                                do_xcom_push=True
                            )
    
    def _print_sql(ti):
        print(ti.xcom_pull(task_ids='sql', key='return_value'))
    
    print_sql = PythonOperator(
                                task_id='print_sql',
                                python_callable=_print_sql,
                                trigger_rule='one_success'  
                            )
    
    process = DummyOperator(
        task_id='process',
    )

    notif_email = DummyOperator(
        task_id='notif_email',
    )

    notif_slack = DummyOperator(
        task_id='notif_slack',
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    start >> create >> insert >> sql >> [process,notif_email,notif_slack] >> print_sql >> end