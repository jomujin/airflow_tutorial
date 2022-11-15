import os
import asyncio
from ...config.condition import DB_DIR


async def apg_copy_table_to_csv(db, schema, table, f=None):
    conn = await db.get_apg_conn()
    result = await conn.copy_from_table(
        table,
        output=f or f"{table}.csv",
        format="csv",
        schema_name=schema,
        header=True,
    )
    print(result)
    await conn.close()

async def apg_copy_from_query_to_csv(db, table, sql, f):
    conn = await db.get_apg_conn()
    result = await conn.copy_from_query(
        sql, 
        output=f or f"{table}.csv", 
        format="csv", 
        header=True)
    print(result)
    await conn.close()

def runApg(apg_func):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(apg_func)
    loop.close()

def cacheTableToCSV(db, schema, table):
    cache_file = f"{DB_DIR}/{table}.csv"
    if not os.path.exists(cache_file):
        with open(cache_file, "wb") as f:
            runApg(apg_copy_table_to_csv(db, schema, table, f))

def cacheTableToCSVByQuery(db, table, sql):
    cache_file = f"{DB_DIR}/{table}.csv"
    if not os.path.exists(cache_file):
        with open(cache_file, "wb") as f:
            runApg(apg_copy_from_query_to_csv(db, table, sql, f))

def download_sub_table(db, schema, table, kb_base_wk):
    cacheTableToCSV(
        db=db,
        schema=schema,
        table=f'{table}_{kb_base_wk}'
    )