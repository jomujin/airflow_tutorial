def get_correct_date(date):
    if date[:2] == '20':
        return date
    else:
        date_arr = date.split('_')
        for i, s in enumerate(date_arr):
            if  s.startswith('20'):
                return '_'.join(date_arr[i:])
    return date

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

def get_partition_date_all(conn, schema, table):
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

def check_is_table_rows(conn, schema, table, mth):
    partition_table_name = f'{table}_{mth}'
    query = f"""
        select exists(
            select * from {schema}.{partition_table_name} limit 1
        )
    """
    return conn.execute(query).fetchone()[0]

def get_recent_partitiondate(conn, schema, table):
    mth_list = get_partition_date_all(conn, schema, table)
    if check_is_table_rows(conn, schema, table, mth_list[0]) == False:
        new_mth_list = list()
        for mth in mth_list:
            if check_is_table_rows(conn, schema, table, mth) == True:
                mth = get_correct_date(mth)
                new_mth_list.append(mth)
        return new_mth_list[0]
    else:
        return get_correct_date(mth_list[0])

def check_is_satisfied_condition(
    conn, 
    schema_1, # 첫번째 테이블(우선순위가 앞인 테이블)이 있는 스키마명
    table_1, # 첫번째 테이블명
    schema_2, # 두번째 테이블(우선순위가 후인 테이블)이 있는 스키마명
    table_2 # 두번째 테이블명
):
    table_1_rpd = get_recent_partitiondate(conn, schema_1, table_1)
    table_2_rpd = get_recent_partitiondate(conn, schema_2, table_2)
    if table_1_rpd == table_2_rpd:
        return (
            False, 
            None
        )
    table_1_pd_list = get_partition_date_all(conn, schema_1, table_1)
    if table_1_pd_list[1] == table_2_rpd:
        return (
            True, 
            table_1_rpd
        )
    return (
            False, 
            None
        )
