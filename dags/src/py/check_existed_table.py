def check_existed_table(conn, schema, table):
    sql = f"""
    select exists (
    select from
        pg_tables
    where 
        schemaname = '{schema}' and 
        tablename  = '{table}'
    )
    """
    res = conn.execute(sql)
    return res.fetchone()[0]