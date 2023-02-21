"""
### Uso del argumento "retries".

Para cada task, Airflow por defecto tiene 0 reintentos, sin mebargo,
existen 3 argumentos asociados a este tema, los cuales son:
    * retries
    * retry_delay
    * retry_exponential_backoff

El primero de ellos nos permite hacer que una task reintente el 
número de veces que se programe.

El segundo argumento nos permite esperar una determinada cantidad 
de tiempo entre un reintento y otro.

Y por último, el tercer argumento, nos va a permitir aumentar 
exponencialmente la cantidad de tiempo en el segundo argumento.

Por último, podemos ingresar a la variable que contiene el número
de intentos realizados en cada task mediante el jinja template:
    * {{ ti.try_number }}

Dentro del comando "echo '{{ ti.try_number }}'" del BashOperator.
"""


from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

with DAG(
            '3_retries',
            start_date=datetime(2023,2,21),
            default_args={"owner":"Andrade"},
            schedule='@daily',
            catchup=False,
            tags=['Curso 4', 'The Operators Guide']
        ) as dag:
    
    dag.doc_md = __doc__

    task_a = BashOperator(owner='Tinmar', task_id='task_a', bash_command="echo 'task_a'")

    task_c = BashOperator(owner='Armando', task_id='task_c', bash_command="echo 'task_c'")

    task_b = BashOperator(
                            task_id='task_b', 
                            bash_command="sleep 5 && exit 1",
                            retries=3,
                            retry_delay=timedelta(seconds=10),
                            retry_exponential_backoff=True
                        )

    task_a >> task_c >> task_b