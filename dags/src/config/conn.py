import asyncpg
import psycopg2
import sqlalchemy
from ...cond.condition import (
    DB_VOS_RAW_DATA,
    DB_AIRFLOW_TUTORIAL
)


class DB_conn:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        database,
    ):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def get_psypg_conn(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
            )
            print("데이터베이스 접속 성공")
        except psycopg2.Error:
            print("데이터베이스 접속 실패. db정보를 확인하세요")
        return conn

    def get_alchmy_conn(self):
        con = sqlalchemy.create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        con = con.execution_options(isolation_level="AUTOCOMMIT")
        return con

    async def get_apg_conn(self):
        return await asyncpg.connect(
            user=self.user,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
        )

CONN_AIRFLOW_TUTORIAL = DB_conn(
    DB_AIRFLOW_TUTORIAL.get('HOST'),
    DB_AIRFLOW_TUTORIAL.get('PORT'),
    DB_AIRFLOW_TUTORIAL.get('USER'),
    DB_AIRFLOW_TUTORIAL.get('PASSWORD'),
    DB_AIRFLOW_TUTORIAL.get('DBNAME')
)

CONN_VOS_RAW_DATA = DB_conn(
    DB_VOS_RAW_DATA.get('HOST'),
    DB_VOS_RAW_DATA.get('PORT'),
    DB_VOS_RAW_DATA.get('USER'),
    DB_VOS_RAW_DATA.get('PASSWORD'),
    DB_VOS_RAW_DATA.get('DBNAME')
)