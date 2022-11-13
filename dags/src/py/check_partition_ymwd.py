def getPartitionDateAll(conn, schema, table):
    sql=f"""
    select 
        table_name 
    from information_schema.tables
    where table_schema = '{schema}'
    and table_name like '{table}%%'
    and table_name != '{table}'
    """
    res = conn.execute(sql)
    mth_list = list()

    for row in res.fetchall():
        mth = row[0].replace(f'{table}_', '')
        mth_list.append(mth)
    mth_list = sorted(mth_list, reverse=True)
    return mth_list

def checkIsTableRows(conn, schema, table, mth):
    partition_table_name = f'{table}_{mth}'
    query = f"""
        select exists(
            select * from {schema}.{partition_table_name} limit 1
        )
    """
    return conn.execute(query).fetchone()[0]

def getRecentPartitionDate(conn, schema, table):
    mth_list = getPartitionDateAll(conn, schema, table)
    if checkIsTableRows(conn, schema, table, mth_list[0]) == False:
        new_mth_list = list()
        for mth in mth_list:
            if checkIsTableRows(conn, schema, table, mth) == True:
                new_mth_list.append(mth)
        return new_mth_list[0]
    else:
        return mth_list[0]

def checkIsSatisfiedCondition(
    conn, 
    schema_1, # 첫번째 테이블(우선순위가 앞인 테이블)이 있는 스키마명
    table_1, # 첫번째 테이블명
    schema_2, # 두번째 테이블(우선순위가 후인 테이블)이 있는 스키마명
    table_2 # 두번째 테이블명
):
    table_1_rpd = getRecentPartitionDate(conn, schema_1, table_1)
    table_2_rpd = getRecentPartitionDate(conn, schema_2, table_2)
    if table_1_rpd == table_2_rpd:
        return False
    table_1_pd_list = getPartitionDateAll(conn, schema_1, table_1)
    if table_1_pd_list[-2] == table_2_rpd:
        return True
    return False
