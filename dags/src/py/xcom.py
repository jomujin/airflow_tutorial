def return_pull_xcom(task_ids, **context):
    xcom_return = context['ti'].xcom_pull(task_ids=task_ids)
    return xcom_return
