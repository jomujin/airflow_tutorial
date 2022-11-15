import airflow
from airflow import DAG, providers_manager
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from nbconvert import python
from src.config.args import DEFAULT_ARGS
from src.py.check_table import check_existed_table
from src.config.conn import CONN_AIRFLOW_TUTORIAL
from src.py.slack_alert import SlackAlert


DAG_ID = "xcom_test"
SLACK = SlackAlert(
    channel='#airflow-slackoperator-test',
    token='xoxb-4356051306278-4356068062918-XiKx8oMkAjhrlF4x3B8Z3M4Y'
)
DAG_PY = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='use case of python operator in airflow',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['example'],
    on_success_callback=SLACK.slack_success_alert,
    on_failure_callback=SLACK.slack_failure_alert
)
conn = CONN_AIRFLOW_TUTORIAL.get_alchmy_conn()

def return_push_xcom(**context):
    return 'hello world!'

def return_pull_xcom(task_ids, **context):
    xcom_return = context['ti'].xcom_pull(task_ids=task_ids)
    return xcom_return

return_push_xcom_op = PythonOperator(
    task_id="return_push_xcom",
    python_callable=return_push_xcom,
    provide_context=True,
    dag = DAG_PY
)

return_pull_xcom_op = PythonOperator(
    task_id="return_pull_xcom",
    python_callable=return_pull_xcom,
    op_kwargs={
        'task_ids':'return_push_xcom'
    },
    provide_context=True,
    dag = DAG_PY
)

return_push_xcom_op >> return_pull_xcom_op
