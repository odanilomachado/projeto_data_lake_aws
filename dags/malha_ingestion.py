from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from modules.malha_operator import MalhaExtractionOperator, MalhaLoadOperator

from datetime import datetime

malha_extraction = MalhaExtractionOperator()
malha_load = MalhaLoadOperator()

current_date = datetime.now().strftime('%Y-%m-%d')

with DAG(
    dag_id= 'data-lake-ingestion',
    tags= ['data-lake-ingestion'],
    start_date = datetime(2025, 1, 10),
    schedule_interval = '0 8 1 * *',
    catchup = False
) as dag:

    start_dag = DummyOperator(
        task_id = 'start_dag'
    )

    extraction_faturas = PythonOperator(
        task_id = 'extraction_faturas',
        python_callable = malha_extraction.start,
        op_args = ['bkt-edj-ped-datalake-dev','ingestion/tb_faturas/']
    )

    extraction_pagamentos = PythonOperator(
        task_id = 'extraction_pagamentos',
        python_callable = malha_extraction.start,
        op_args = ['bkt-edj-ped-datalake-dev','ingestion/tb_pagamentos/']
    )

    wait_dag = DummyOperator(
        task_id = 'wait_dag'
    )

    load_faturas = PythonOperator(
        task_id = 'load_faturas',
        python_callable = malha_load.start,
        op_args = ['tb_faturas', 'lake-project-']
    )

    load_pagamentos = PythonOperator(
        task_id = 'load_pagamentos',
        python_callable = malha_load.start,
        op_args = ['tb_pagamentos', 'lake-project-']
    )

    end_dag = DummyOperator(
        task_id = 'end_dag'
    )

    start_dag >> [extraction_faturas, extraction_pagamentos] >> wait_dag >> [load_faturas, load_pagamentos] >> end_dag
    
    