"""
### Uso del ExternalTaskSensor

Este sensor no va a permitir esperar a que finalice una tarea en un 
dag externo. 

Sin embargo, es necesario que las fechas del DAG Run coincidan entre
el TaskSensor y la tarea en el DAG externo.

En esta ocasión, no funcionó el sensor debido a que no pude igualar
las fechas entre el sensor y la tarea del DAG externo.
"""

from airflow import DAG 
from datetime import datetime

from airflow.operators.dummy import DummyOperator 
from airflow.sensors.external_task import ExternalTaskSensor


default_args={
    "owner":"Tinmar",
    "start_date":datetime(2023,2,2)
}

with DAG(
            '14_external_sensor',
            catchup=False,
            default_args=default_args,
            schedule='@daily',
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    start = DummyOperator(task_id='start')

    sensor = ExternalTaskSensor(
                                    task_id='sensor',
                                    external_dag_id='13.1_trigger_Dag',
                                    external_task_id='end',
                                    poke_interval=15   
                                )

    end = DummyOperator(task_id='end')

    start >> sensor >> end