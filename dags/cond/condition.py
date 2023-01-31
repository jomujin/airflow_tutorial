import os
import sqlalchemy
import dotenv


dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

# EMAIL
EMAIL = os.environ.get('EMAIL')

# PATH
PATH = os.getcwd()
DB_DIR = "db" 
DB_PATH = PATH + '/' + DB_DIR

# DB CONNECTION
DB_AIRFLOW_TUTORIAL = {
    'HOST' : os.environ.get('DB_AIRFLOW_TUTORIAL_HOST'),
    'PORT' : os.environ.get('DB_AIRFLOW_TUTORIAL_PORT'),
    'USER' : os.environ.get('DB_AIRFLOW_TUTORIAL_USER'),
    'PASSWORD' : os.environ.get('DB_AIRFLOW_TUTORIAL_PASSWORD'),
    'DBNAME' : os.environ.get('DB_AIRFLOW_TUTORIAL_DBNAME')
}

DB_VOS_RAW_DATA = {
    'HOST' : os.environ.get('DB_VOS_RAW_DATA_HOST'),
    'PORT' : os.environ.get('DB_VOS_RAW_DATA_PORT'),
    'USER' : os.environ.get('DB_VOS_RAW_DATA_USER'),
    'PASSWORD' : os.environ.get('DB_VOS_RAW_DATA_PASSWORD'),
    'DBNAME' : os.environ.get('DB_VOS_RAW_DATA_DBNAME')
}

AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL = os.environ.get('AIRFLOW_CONN_POSTGRES_AIRFLOW_TUTORIAL')

# SLACK
SLACK_CHANNEL = os.environ.get('SLACK_CHANNEL')
SLACK_TOKEN = os.environ.get('SLACK_TOKEN')


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
