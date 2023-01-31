import numpy as np
import pandas as pd
from ....cond.condition import (
    DB_PATH,
    KB_SCHEMA,
    KB_PY_INFO_TABLE,
    KB_PY_INFO_SQL_DTYPES
)
from .common_helper import (
    check_duration
)


def update_to_sql(data, conn, schema, table, if_exists, dtypes):
    data = data.fillna(value=np.nan)
    data.to_sql(
        name = table,
        con = conn,
        schema = schema,
        if_exists = if_exists,
        index = False,
        dtype = dtypes
    )

def update_partition_table(conn, data, schema, table, dtypes, kb_base_wk):
    schema = schema
    if_exists = 'append'
    dtypes = dtypes
  
    if 'index' in list(data.columns):
        data = data.drop('index', axis=1)
    if 'idx' in list(data.columns):
        data = data.drop('idx', axis=1)

    partition_table = f'{table}_{kb_base_wk}'
    update_to_sql(data, conn, schema, partition_table, if_exists, dtypes)
    # try:
    #     update_to_sql(data, conn, schema, partition_table, if_exists, dtypes)
    #     print(f"Complete Update {partition_table} to RAW_DATA.{schema} DB")
    # except:
    #     print(f'Failed Update {partition_table} to RAW_DATA.{schema} DB')

def is_index_exists(conn, schema, table, column):
    query = f"""
    select 
        count(*)
    from pg_catalog.pg_indexes 
    where schemaname = '{schema}'
    and tablename = '{table}'
    and indexname = '{table}_{column}'
    """
    res = conn.execute(query)
    return res.fetchall()[0][0]

def is_table_exists(conn, schema, table):
    query = f"""
    select 
        count(*)
    from information_schema.tables
    where table_schema = '{schema}'
    and table_name = '{table}'
    """
    res = conn.execute(query)
    return res.fetchall()[0][0]

def createIndex(conn, schema, table, column):
    if is_index_exists(schema, table):
        if ~is_index_exists(schema, table, column):
            query = f"""
            create index {table}_{column} on {schema}.{table}({column})
            """
            conn.execute(query)
            print(f"Succeed Creating Index {table}_{column} in {table}")
        else:
            print(f"Existed Index {table}_{column} in {table}")
    else:
        print(f"Not Existed Table {table}")

def create_mother_table(conn, schema, table, query_dtypes, partition_col):
    sql_query = f"""
    DROP TABLE IF EXISTS {schema}.{table};
    CREATE TABLE {schema}.{table} ({query_dtypes})
    PARTITION BY list ({partition_col});
    """
    conn.execute(sql_query)
    print(f"Succeed Created Table {table} in {schema}")

def create_partition_table(conn, schema, table, partition_name, partition_type):
    partition_table = f'{table}_{partition_name}'
    create_table_sql = f"""
        DROP TABLE IF EXISTS {schema}.{partition_table} CASCADE;
        CREATE TABLE {schema}.{partition_table}
        PARTITION OF {schema}.{table}
        FOR VALUES IN ('{partition_name}'::{partition_type});
    """
    try:
        conn.execute(create_table_sql)
        print(f'Succeed Create Partiton {partition_table} Table')
    except:
        print(f'Failed Create Partiton {partition_table} Table')

# KB 평형별 가격데이터 전유공용 일원화 매칭
@check_duration
def update_kb_py_info(conn, table, kb_base_wk):
    data = pd.read_csv(
        f'{DB_PATH}/{KB_PY_INFO_TABLE}_{kb_base_wk}.csv',
        encoding='utf-8',
        dtype={
            'kb_complex_code' : str,
            'kb_complex_name' : str,
            'kb_approval_ym' : str,
            'kb_occupancy_ym' : str,
            'kb_type_code' : str,
            'kb_area_jy' : float,
            'kb_area_contract' : float,
            'kb_etc_area_jy' : str,
            'kb_pyeong_typ' : str,
            'kb_pyeong_typ_main' : int,
            'kb_pyeong_typ_sub' : str,
            'kb_rm_cnt' : int,
            'kb_brm_cnt' : int,
            'kb_dir' : str,
            'kb_ent_strc' : str,
            'kb_price' : float,
            'kb_lower_price' : float,
            'kb_upper_price' : float,
            'kb_jeonse_price' : float,
            'kb_lower_jeonse_price' : float,
            'kb_upper_jeonse_price' : float,
            'kb_mth_rent_deposit' : float,
            'kb_lower_mth_rent_price' : float,
            'kb_upper_mth_rent_price' : float,
            'kb_base_dt' : str,
            'kb_limit_dt' : str,
            'kb_is_not_prov' : str,
            'kb_base_wk' : str
        }
    )
    schema = KB_SCHEMA
    table = KB_PY_INFO_TABLE
    dtypes = KB_PY_INFO_SQL_DTYPES
    partition_table = f'{table}_{kb_base_wk}'
    query_dtypes = """
        kb_complex_code text,
        kb_complex_name text,
        kb_approval_ym text,
        kb_occupancy_ym text,
        kb_type_code text,
        kb_area_jy float,
        kb_area_contract float,
        kb_etc_area_jy text,
        kb_pyeong_typ text,
        kb_pyeong_typ_main integer,
        kb_pyeong_typ_sub text,
        kb_rm_cnt integer,
        kb_brm_cnt integer,
        kb_dir text,
        kb_ent_strc text,
        kb_price float,
        kb_lower_price float,
        kb_upper_price float,
        kb_jeonse_price float,
        kb_lower_jeonse_price float,
        kb_upper_jeonse_price float,
        kb_mth_rent_deposit float,
        kb_lower_mth_rent_price float,
        kb_upper_mth_rent_price float,
        kb_base_dt date,
        kb_limit_dt date,
        kb_is_not_prov integer,
        kb_base_wk text NOT NULL
        """
    partition_col = 'kb_base_wk'

    if is_table_exists(conn, schema, table)!=1:
        create_mother_table(
            conn=conn,
            schema=schema, 
            table=table, 
            query_dtypes=query_dtypes, 
            partition_col=partition_col
        )
        createIndex(
            schema=schema, 
            table=table, 
            column='kb_complex_code'
        )
        createIndex(
            schema=schema, 
            table=table, 
            column='kb_type_code'
        )
    if is_table_exists(conn, schema, partition_table)!=1:
        create_partition_table(
            conn=conn,
            schema=schema, 
            table=table, 
            partition_name=kb_base_wk, 
            partition_type='text'
        )
        try:
            update_partition_table(
                data=data, 
                conn=conn,
                schema=schema, 
                table=table, 
                dtypes=dtypes, 
                kb_base_wk=kb_base_wk
            )
            # print("to_sql duration: {} seconds".format(time.time() - start_time))
        except:
            pass