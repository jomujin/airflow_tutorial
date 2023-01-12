from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from datetime import datetime, timedelta
from src.config.args import DEFAULT_ARGS
from src.config.conn import CONN_AIRFLOW_TUTORIAL
from cond import (
    KB_SCHEMA,
    KB_RAW_COMPLEX_TABLE,
    KB_RAW_PYTYPE_TABLE,
    KB_RAW_PRICE_TABLE,
    KB_COMPLEX_PNU_MAP_TABLE,
    KB_PY_INFO_TABLE,
    SLACK_CHANNEL,
    SLACK_TOKEN
)
from src.py.helper.slack_helper import (
    SlackAlert
)
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
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN
)
DAG_PY = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='use case of python operator in airflow',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(seconds=30),
    catchup=False, # 정해진 시간에 실행되지 못한 DAG를 늦게라도 실행하는 것
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
        return 'skip_task' # 서브 테이블 유무 불만족하면 False
    else: # 업데이트 조건을 불만족하면 False
        return 'skip_task'

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

def skip_func():
    return 'skip'

def check_branch(**context):
    return None

def finish_func(**context):
    res = return_pull_xcom(task_ids='check_is_satisfied_condition_task', **context)
    if res == 'skip_task':
        return 'Stoped Update Due to Unsatisfied Condition'
    else:
        return 'Succeded Update Kb_py_info Table'

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
    provide_context=True,
    dag=DAG_PY
)

create_kb_py_info_op = PythonOperator(
    task_id='create_kb_py_info_task',
    python_callable=pull_kb_base_wk_and_create_kb_py_info_table,
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
    provide_context=True,
    dag=DAG_PY
)

skip_op = PythonOperator(
    task_id='skip_task',
    python_callable=skip_func,
    provide_context=True,
    dag=DAG_PY
)

check_branch_op = PythonOperator(
    task_id='check_branch_task',
    python_callable=check_branch,
    trigger_rule="one_success", # 앞 작업 성공시, 분기되어 있으므로 둘 중 하나만 성공해도 진행됨
    provide_context=True,
    dag=DAG_PY
)

finish_op = PythonOperator(
    task_id='finish_task',
    python_callable=finish_func,
    trigger_rule="all_done", # 작업 성공 여부에 관계없이 모두 작동한 경우
    provide_context=True,
    dag=DAG_PY
)

branch_check_condition_op >> [
    return_kb_base_wk_op, 
    skip_op
]
return_kb_base_wk_op >> [
    download_kb_complex_op, 
    download_kb_peongtype_op,
    download_kb_price_op,
    download_kb_complex_pnu_map_op
] >> create_kb_py_info_op >> update_kb_py_info_op >> check_branch_op
skip_op >> check_branch_op
check_branch_op >> finish_op
