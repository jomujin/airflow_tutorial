import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from src.config.conn import AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL
from src.config.args import DEFAULT_ARGS

DAG_ID = "connect_postgres"
DAG_PSQL = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='use case of psql operator in airflow',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=['/Users/macrent/airflow/dags/src/sql'],
    catchup=False,
    tags=['example']
)

create_table = PostgresOperator(
    sql = 'create_table.sql',
    task_id = "create_table_query",
    postgres_conn_id = AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL,
    dag = DAG_PSQL
)

insert_data = PostgresOperator(
    sql = 'insert_table.sql',
    task_id = "insert_table_query",
    postgres_conn_id = AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL,
    dag = DAG_PSQL
)

create_table >> insert_data