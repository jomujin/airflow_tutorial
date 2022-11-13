import psycopg2
import sqlalchemy


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

CONN_AIRFLOW_TUTORIAL = DB_conn('localhost', 5432, 'test', '1234', 'airflow_tutorial')
CONN_VOS_RAW_DATA = DB_conn('34.64.221.21', 5432, 'dev_mjjo', '1234', 'raw-data')
AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL = 'postgres_airflow_tutorial'
