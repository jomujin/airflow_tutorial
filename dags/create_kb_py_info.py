from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from datetime import datetime, timedelta
from src.config.args import DEFAULT_ARGS
from src.config.conn import CONN_AIRFLOW_TUTORIAL
from src.py.slack_alert import SlackAlert
from src.py.check_table import (
    check_is_satisfied_condition
)


DAG_ID = "create_kb_py_info"
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


def branch_check_is_satisfied_condition(**kwargs):
    res = check_is_satisfied_condition(
        conn=kwargs['conn'],
        schema_1=kwargs['schema_1'],
        table_1=kwargs['table_1'],
        schema_2=kwargs['schema_2'],
        table_2=kwargs['table_2']
    )
    # 다음에 실행할 task_id 를 반환한다.
    if res: # 조건을 만족하면 True
        return 'test_continue_task'
    else: # 조건을 불만족하면 False
        return 'test_stop_task'

def continue_func():
    return 'continue'

def stop_func():
    return 'stop'

branch_check_condition_op = BranchPythonOperator(
    task_id="check_is_satisfied_condition",
    python_callable=branch_check_is_satisfied_condition,
    op_kwargs={
        'conn': conn,
        'schema_1': 'kb',
        'table_1': 'kb_complex',
        'schema_2': 'kb',
        'table_2': 'kb_py_info'
    },
    trigger_rule="all_done",
    provide_context=True,
    dag = DAG_PY
)

continue_op = PythonOperator(
    task_id='test_continue_task',
    python_callable=continue_func,
    dag=DAG_PY
)

stop_op = PythonOperator(
    task_id='test_stop_task',
    python_callable=stop_func,
    dag=DAG_PY
)

branch_check_condition_op >> [continue_op, stop_op]
