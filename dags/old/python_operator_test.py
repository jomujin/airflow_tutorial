import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from src.config.args import DEFAULT_ARGS
from src.py.helper.tb_check_helper import check_existed_table
from src.config.conn import CONN_AIRFLOW_TUTORIAL
from src.py.helper.slack_helper import SlackAlert


DAG_ID = "test_pythonoperator"
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

check_table = PythonOperator(
    task_id="check_existed_table_py",
    python_callable=check_existed_table,
    op_kwargs={
        'conn': conn,
        'schema': 'kb',
        'table': 'test'
    },
    trigger_rule="all_done",
    dag = DAG_PY
)

check_table
