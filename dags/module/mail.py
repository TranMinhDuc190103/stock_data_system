from airflow.operators.email import EmailOperator

def email_on_failure(context):
    ti = context['task_instance']
    email = EmailOperator(
        task_id="email_on_failure",
        to='scarletmoon2003@gmail.com',
        subject=f"Airflow Alert: {ti.task_id} failed on {ti.dag_id}",
        html_content=f"Task {ti.task} failed on DAG {ti.dag_id}.",
    )
    email.execute(context)
    
def email_on_success(context):
    ti = context['task_instance']
    email = EmailOperator(
        task_id="email_on_success",
        to='scarletmoon2003@gmail.com',
        subject=f"Airflow Alert: {ti.task_id} on {ti.dag_id} successfully operated.",
        html_content=f"Task {ti.task} on DAG {ti.dag_id} successfully operated."
    )
    email.execute(context)
    
    