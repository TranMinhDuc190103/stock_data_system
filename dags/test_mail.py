from airflow import DAG
from airflow.operators.empty import EmptyOperator

from dags.module import mail
default_arg = {
    "Owner": "airflow"
}

dag = DAG(
    dag_id='test_mail',
    on_success_callback=mail.email_on_success,
    on_failure_callback=mail.email_on_failure,
    default_args=default_arg
)

task_1 = EmptyOperator(task_id='task_1', dag=dag)

task_2 = EmptyOperator(task_id='task_2', dag=dag)

task_3 = EmptyOperator(task_id='task_3', dag=dag, on_success_callback=mail.email_on_success)

task_1 >> task_2 >> task_3