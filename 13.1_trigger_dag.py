"""
### Uso del TriggerDagRunOperator

Este operador nos permite ejecutar un DAG de manera "remota" desde 
otro DAG.

Los primeros parámetros importantes que encontramos son:
    
    * trigger_dag_id
    * execution_date

El primero define el DAG que deseamos ejecutar de manera remota, el
segundo nos dice en qué fecha se ejecutará el DAG.

Por otra parte, tenemos los siguientes parámetros:

    * reset_dag_run
    * conf
    * wait_for_completion
    * poke_interval

El primero nos va a permitir generar la ejecución de un DAG incluso
cuando dicho DAG ya tenga un DAG Run en la misma fecha, siempre que
esté definido como True. 
En caso de definirse False, no se podrá correr dos veces en una 
misma fecha el mismo DAG.

El segundo es un parámetro que nos va a permitir pasar variables
mediante el uso de un diccionario.

El tercero hace referencia a que la tarea va a finalizar hasta que 
la ejecución del DAG externo termine, por lo que el DAG principal
continuará con las demás tareas solo cuando el DAG externo haya 
completado sus ejecución.

El último de ellos define el tiempo en segundos que tarda el 
operador en verificar si el DAG externo terminó de ejecutarse o no.
"""

from airflow import DAG
from datetime import datetime, timezone

from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner":"Tinmar",
    "start_date":datetime(2023,2,2)
}

with DAG(
            '13.1_trigger_dag',
            catchup=False,
            schedule='@daily',
            default_args=default_args,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    start = DummyOperator(task_id='start')

    trigger_1 = TriggerDagRunOperator(
        task_id='trigger_1',
        trigger_dag_id='13.2_target_dag',
        execution_date=datetime(2023,2,27,tzinfo=timezone.utc),
        reset_dag_run=True,
        conf={
            'path':'/c/Airflow/dags'
        },
        wait_for_completion=True,
        poke_interval=20
    )

    trigger_2 = TriggerDagRunOperator(
        task_id='trigger_2',
        trigger_dag_id='12_task_group',
        execution_date=datetime(2023,2,27,tzinfo=timezone.utc),
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15
    )

    end = DummyOperator(task_id='end')

    start >> [trigger_1,trigger_2] >> end
