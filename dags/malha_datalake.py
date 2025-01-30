from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from modules.malha_operator import MalhaProcessOperator, MalhaBookOperator

from datetime import datetime

malha_process = MalhaProcessOperator()
malha_book = MalhaBookOperator()

current_date = datetime.now().strftime('%Y-%m-%d')

with DAG(
    dag_id= 'data-lake-aws-process',
    tags= ['data-lake-process'],
    start_date = datetime(2025, 1, 10),
    schedule_interval = '0 8 5 * *',
    catchup = False
) as dag:

    start_dag = DummyOperator(
        task_id = 'start_dag'
    )

    process_fatura = PythonOperator(
        task_id = 'process_fatura',
        python_callable = malha_process.start,
        op_args = ['tb_faturas']
    )

    process_pagamentos = PythonOperator(
        task_id = 'process_pagamentos',
        python_callable = malha_process.start,
        op_args = ['tb_pagamentos']
    )

    process_book = PythonOperator(
        task_id = 'process_book',
        python_callable = malha_book.start,
        op_args = ['book_fatura']
    )

    end_dag = DummyOperator(
        task_id = 'end_dag'
    )

    start_dag >> [process_fatura, process_pagamentos] >> process_book >> end_dag