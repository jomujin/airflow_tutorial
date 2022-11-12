import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from src.config.conn import AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL
from src.config.args import DEFAULT_ARGS
from src.py.check_existed_table import check_existed_table


DAG_ID = "test_pythonoperator"
DAG_PY = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='use case of python operator in airflow',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    # template_searchpath=['/Users/macrent/airflow/src/py'],
    catchup=False,
    tags=['example']
)

create_table = PythonOperator(
    task_id = "check_existed_table_py",
    python_callable=check_existed_table,
    op_kwargs={
        'schema':'kb',
        'table':'test'
    },
    dag = DAG_PY
)

create_table
