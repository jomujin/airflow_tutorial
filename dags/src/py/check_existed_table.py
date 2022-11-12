from ..config.conn import CONN_AIRFLOW_TUTORIAL


def check_existed_table(schema, table):
    conn = CONN_AIRFLOW_TUTORIAL.getPsypgConn()
    curr = conn.cursor()
    sql = f"""
    select exists (
    select from
        pg_tables
    where 
        schemaname = '{schema}' and 
        tablename  = '{table}'
    )
    """
    curr.execute(sql)
    return curr.fetchone()[0]