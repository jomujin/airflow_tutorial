from slack_sdk import WebClient
from datetime import datetime


class SlackAlert:
    def __init__(self, channel, token):
        self.slack_channel = channel
        self.token = token
        self.client = WebClient(token=self.token)
        
    def slack_success_alert(self, context):
        text = f"""
        *Alert* : Success!
        *Date* : {datetime.today().strftime('%Y-%m-%d, %H:%M:%S')}
        *Task Id* : {context.get('task_instance').task_id},
        *Dag Id*: {context.get('task_instance').dag_id},
        *Log Url* : <{context.get('task_instance').log_url}|Logs>
        """
        self.client.chat_postMessage(channel=self.slack_channel, text=text)

    def slack_failure_alert(self, context):
        text = f"""
        *Alert* : Fail!
        *Date* : {datetime.today().strftime('%Y-%m-%d, %H:%M:%S')}
        *Task Id* : {context.get('task_instance').task_id},
        *Dag Id*: {context.get('task_instance').dag_id},
        *Log Url* : <{context.get('task_instance').log_url}|Logs>
        """
        self.client.chat_postMessage(channel=self.slack_channel, text=text)
