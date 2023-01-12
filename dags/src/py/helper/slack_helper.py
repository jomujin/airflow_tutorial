from slack_sdk import WebClient
from datetime import datetime
from .xcom_helper import (
    return_pull_xcom
)


class SlackAlert:
    def __init__(self, channel, token):
        self.slack_channel = channel
        self.token = token
        self.client = WebClient(token=self.token)

    def slack_success_alert(self, context):
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        log_url = context.get('task_instance').log_url
        # result_msg = return_pull_xcom(task_ids=f'{task_id}', **context)
        text = f"""
        *Alert* : Success!
        *Date* : {datetime.today().strftime('%Y-%m-%d, %H:%M:%S')}
        *Task Id* : {task_id},
        *Dag Id*: {dag_id},
        *Log Url* : <{log_url}|Logs>
        """
        # *Result Msg* : {result_msg}
        self.client.chat_postMessage(channel=self.slack_channel, text=text)

    def slack_failure_alert(self, context):
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        log_url = context.get('task_instance').log_url
        # result_msg = return_pull_xcom(task_ids=f'{task_id}', **context)
        text = f"""
        *Alert* : Fail!
        *Date* : {datetime.today().strftime('%Y-%m-%d, %H:%M:%S')}
        *Task Id* : {task_id},
        *Dag Id*: {dag_id},
        *Log Url* : <{log_url}|Logs>
        """
        self.client.chat_postMessage(channel=self.slack_channel, text=text)
