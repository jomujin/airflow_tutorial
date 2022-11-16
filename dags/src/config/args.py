from datetime import timedelta

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['mj.jo@valueofspace.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=1),
}