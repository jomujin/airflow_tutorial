from datetime import timedelta
from .condition import EMAIL


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': [EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}