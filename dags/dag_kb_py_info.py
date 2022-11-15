from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from datetime import datetime, timedelta
from src.config.args import DEFAULT_ARGS
from src.config.conn import CONN_AIRFLOW_TUTORIAL
from src.config.condition import (
    KB_SCHEMA,
    KB_RAW_COMPLEX_TABLE,
    KB_RAW_PYTYPE_TABLE,
    KB_RAW_PRICE_TABLE,
    KB_COMPLEX_PNU_MAP_TABLE,
    KB_PY_INFO_TABLE
)
from src.py.helper.slack_helper import SlackAlert
from src.py.helper.tb_check_helper import (
    check_existed_table,
    check_is_satisfied_condition,
    get_recent_partitiondate
)
from src.py.helper.tb_download_helper import (
    download_sub_table
)
from src.py.helper.xcom_helper import (
    return_pull_xcom
)
from src.py.creator.kb_py_info import (
    create_kb_py_info_table
)
from src.py.helper.tb_upload_helper import (
    update_kb_py_info
)


DAG_ID = "create_kb_py_info"
SLACK = SlackAlert(
    channel='#airflow-slackoperator-test',
    token='xoxb-4356051306278-4356068062918-PoL2IMASKfSMps7tTwHw3Jr0'
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

def branch_check_existed_all_sub_table_func(kb_base_wk):
    res_1 = check_existed_table(conn, KB_SCHEMA, f'{KB_RAW_COMPLEX_TABLE}_{kb_base_wk}')
    res_2 = check_existed_table(conn, KB_SCHEMA, f'{KB_RAW_PYTYPE_TABLE}_{kb_base_wk}')
    res_3 = check_existed_table(conn, KB_SCHEMA, f'{KB_RAW_PRICE_TABLE}_{kb_base_wk}')
    res_4 = check_existed_table(conn, KB_SCHEMA, f'{KB_COMPLEX_PNU_MAP_TABLE}_{kb_base_wk}')
    if res_1 and res_2 and res_3 and res_4:
        return True
    return False

def branch_check_is_satisfied_condition_func(**kwargs):
    res, kb_base_wk = check_is_satisfied_condition(
        conn=kwargs['conn'],
        schema_1=kwargs['schema_1'],
        table_1=kwargs['table_1'],
        schema_2=kwargs['schema_2'],
        table_2=kwargs['table_2']
    )
    # 다음에 실행할 task_id 를 반환한다.
    if res: # 업데이트 조건을 만족하면 True
        if branch_check_existed_all_sub_table_func(kb_base_wk): # 서브 테이블 유무 만족하면 True
            return 'return_kb_base_wk_task'
        return 'test_stop_task' # 서브 테이블 유무 불만족하면 False
    else: # 업데이트 조건을 불만족하면 False
        return 'test_stop_task'

def return_kb_base_wk(conn, schema, table, **context):
    kb_base_wk = get_recent_partitiondate(conn, schema, table)
    return kb_base_wk

def pull_kb_base_wk_and_download_sub_table(db, schema, table, **context):
    kb_base_wk = return_pull_xcom(task_ids='return_kb_base_wk_task', **context)
    download_sub_table(db, schema, table, kb_base_wk)

def pull_kb_base_wk_and_create_kb_py_info_table(**context):
    kb_base_wk = return_pull_xcom(task_ids='return_kb_base_wk_task', **context)
    create_kb_py_info_table(kb_base_wk)

def pull_kb_base_wk_and_update_kb_py_info_table(conn, table, **context):
    kb_base_wk = return_pull_xcom(task_ids='return_kb_base_wk_task', **context)
    update_kb_py_info(conn, table, kb_base_wk)

def stop_func():
    return 'stop'

branch_check_condition_op = BranchPythonOperator(
    task_id="check_is_satisfied_condition_task",
    python_callable=branch_check_is_satisfied_condition_func,
    op_kwargs={
        'conn': CONN_AIRFLOW_TUTORIAL.get_alchmy_conn(),
        'schema_1': KB_SCHEMA,
        'table_1': KB_RAW_PRICE_TABLE,
        'schema_2': KB_SCHEMA,
        'table_2': KB_PY_INFO_TABLE
    },
    trigger_rule="all_done",
    provide_context=True,
    dag = DAG_PY
)

return_kb_base_wk_op = PythonOperator(
    task_id='return_kb_base_wk_task',
    python_callable=return_kb_base_wk,
    op_kwargs={
        'conn': CONN_AIRFLOW_TUTORIAL.get_alchmy_conn(),
        'schema': KB_SCHEMA,
        'table': KB_RAW_PRICE_TABLE
    },
    provide_context=True,
    dag=DAG_PY
)

download_kb_complex_op = PythonOperator(
    task_id='download_kb_complex_task',
    python_callable=pull_kb_base_wk_and_download_sub_table,
    op_kwargs={
        'db': CONN_AIRFLOW_TUTORIAL,
        'schema': KB_SCHEMA,
        'table': KB_RAW_COMPLEX_TABLE,
    },
    trigger_rule="all_done",
    provide_context=True,
    dag=DAG_PY
)

download_kb_peongtype_op = PythonOperator(
    task_id='download_kb_peongtype_task',
    python_callable=pull_kb_base_wk_and_download_sub_table,
    op_kwargs={
        'db': CONN_AIRFLOW_TUTORIAL,
        'schema': KB_SCHEMA,
        'table': KB_RAW_PYTYPE_TABLE,
    },
    trigger_rule="all_done",
    provide_context=True,
    dag=DAG_PY
)

download_kb_price_op = PythonOperator(
    task_id='download_kb_price_task',
    python_callable=pull_kb_base_wk_and_download_sub_table,
    op_kwargs={
        'db': CONN_AIRFLOW_TUTORIAL,
        'schema': KB_SCHEMA,
        'table': KB_RAW_PRICE_TABLE,
    },
    trigger_rule="all_done",
    provide_context=True,
    dag=DAG_PY
)

download_kb_complex_pnu_map_op = PythonOperator(
    task_id='download_kb_complex_pnu_map_task',
    python_callable=pull_kb_base_wk_and_download_sub_table,
    op_kwargs={
        'db': CONN_AIRFLOW_TUTORIAL,
        'schema': KB_SCHEMA,
        'table': KB_COMPLEX_PNU_MAP_TABLE,
    },
    trigger_rule="all_done",
    provide_context=True,
    dag=DAG_PY
)

create_kb_py_info_op = PythonOperator(
    task_id='create_kb_py_info_task',
    python_callable=pull_kb_base_wk_and_create_kb_py_info_table,
    trigger_rule="all_done",
    provide_context=True,
    dag=DAG_PY
)

update_kb_py_info_op = PythonOperator(
    task_id='update_kb_py_info_task',
    python_callable=pull_kb_base_wk_and_update_kb_py_info_table,
    op_kwargs={
        'conn': CONN_AIRFLOW_TUTORIAL.get_alchmy_conn(),
        'table': KB_PY_INFO_TABLE
    },
    trigger_rule="all_done",
    provide_context=True,
    dag=DAG_PY
)

stop_op = PythonOperator(
    task_id='test_stop_task',
    python_callable=stop_func,
    dag=DAG_PY
)

finish_op = EmptyOperator(
    task_id='finish_task',
    dag=DAG_PY
)

branch_check_condition_op >> [
    return_kb_base_wk_op, 
    stop_op
]
return_kb_base_wk_op >> [
    download_kb_complex_op, 
    download_kb_peongtype_op,
    download_kb_price_op,
    download_kb_complex_pnu_map_op
] >> create_kb_py_info_op >> update_kb_py_info_op >> finish_op
stop_op >> finish_op
