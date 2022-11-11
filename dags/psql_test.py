import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from config.conn import AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL
from config.args import DEFAULT_ARGS

DAG_ID = "connect_postgres"
DAG_PSQL = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='use case of psql operator in airflow',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['example']
)

create_table_sql_query = """
create table kb.test(
    asset_pnu varchar(19) not null,
    kb_complex_code varchar
)
"""

insert_table_sql_query = """
insert into kb.test(
    asset_pnu,
    kb_complex_code
)
values
('4827010800107280041', 'KBA000001'),
('4413311000103770010', 'KBA000002'),
('2823710100107990005', 'KBA000003')
"""

create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table_query",
    postgres_conn_id = AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL,
    dag = DAG_PSQL
)

insert_data = PostgresOperator(
    sql = insert_table_sql_query,
    task_id = "insert_table_query",
    postgres_conn_id = AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL,
    dag = DAG_PSQL
)

create_table >> insert_data