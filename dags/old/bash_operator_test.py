from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
from ..cond.condition import EMAIL

local_tz = pendulum.timezone('Asia/Seoul')

with DAG(
    'tutorial',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': [f'{EMAIL}'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

        t1 =  BashOperator(
                task_id='print_date',
                bash_command='date',
                dag=dag,
        )

        t2 = BashOperator(
                task_id='sleep',
                depends_on_past=False,
                bash_command='sleep 5',
                dag=dag,
        )

        templated_command = """

        {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
        {% endfor %}

        """

        t3 = BashOperator(
                task_id='templated',
                depends_on_past=False,
                bash_command=templated_command,
                params={'my_param': 'Parameter I passed in'},
                dag=dag,
        )
        
        t1 >> [t2, t3]