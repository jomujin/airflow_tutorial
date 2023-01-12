import os
import sqlalchemy

EMAIL = 'mj.jo@valueofspace.com'

PATH = os.getcwd()
DB_DIR = "db" 
DB_PATH = PATH + '/' + DB_DIR

# SCHEMA
KB_SCHEMA = "kb"

# TABLE
KB_RAW_COMPLEX_TABLE = "kb_complex"
KB_RAW_PYTYPE_TABLE = "kb_peongtype"
KB_RAW_PRICE_TABLE = "kb_price"
KB_COMPLEX_PNU_MAP_TABLE = "kb_complex_pnu_map"
KB_PY_INFO_TABLE = "kb_py_info"

KB_PY_INFO_SQL_DTYPES = {
    'kb_complex_code' : sqlalchemy.types.Text(),
    'kb_complex_name' : sqlalchemy.types.Text(),
    'kb_approval_ym' : sqlalchemy.types.Text(),
    'kb_occupancy_ym' : sqlalchemy.types.Text(),
    'kb_type_code' : sqlalchemy.types.Text(),
    'kb_area_jy' : sqlalchemy.types.Float(),
    'kb_area_contract' : sqlalchemy.types.Float(),
    'kb_etc_area_jy' : sqlalchemy.types.Text(),
    'kb_pyeong_typ' : sqlalchemy.types.Text(),
    'kb_pyeong_typ_main' : sqlalchemy.types.Integer(),
    'kb_pyeong_typ_sub' : sqlalchemy.types.Text(),
    'kb_rm_cnt' : sqlalchemy.types.Integer(),
    'kb_brm_cnt' : sqlalchemy.types.Integer(),
    'kb_dir' : sqlalchemy.types.Text(),
    'kb_ent_strc' : sqlalchemy.types.Text(),
    'kb_price' : sqlalchemy.types.Float(),
    'kb_lower_price' : sqlalchemy.types.Float(),
    'kb_upper_price' : sqlalchemy.types.Float(),
    'kb_jeonse_price' : sqlalchemy.types.Float(),
    'kb_lower_jeonse_price' : sqlalchemy.types.Float(),
    'kb_upper_jeonse_price' : sqlalchemy.types.Float(),
    'kb_mth_rent_deposit' : sqlalchemy.types.Float(),
    'kb_lower_mth_rent_price' : sqlalchemy.types.Float(),
    'kb_upper_mth_rent_price' : sqlalchemy.types.Float(),
    'kb_base_dt' : sqlalchemy.types.Date(),
    'kb_limit_dt' : sqlalchemy.types.Date(),
    'kb_is_not_prov' : sqlalchemy.types.Integer(),
    'de_base_wk' : sqlalchemy.types.Text()
}

DB_AIRFLOW_TUTORIAL = {
    'HOST' : 'localhost', 
    'PORT' : 5432, 
    'USER' : 'test', 
    'PASSWORD' : '1234', 
    'DBNAME' : 'airflow_tutorial'
}

DB_VOS_RAW_DATA = {
    'HOST' : '34.64.221.21', 
    'PORT' : 5432, 
    'USER' : 'dev_mjjo', 
    'PASSWORD' : '1234', 
    'DBNAME' : 'raw-data'
}

AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL = 'postgres_airflow_tutorial'
