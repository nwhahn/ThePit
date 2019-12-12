import requests
from enum import Enum


class Alert(Enum):
    INFO = 1
    WARN = 2
    ERROR = 3


groupme_annoyance_mapper = {
    Alert.INFO: 'INFO',
    Alert.WARN: 'WARN',
    Alert.ERROR: 'ERROR'
}


class Alerting:
    def __init__(self):
        self.alert_dict = {}
        self.alert_level = Alert.INFO

    def info(self, message: str):
        self.alert_dict[message] = 'INFO'

    def warn(self, message: str):
        self.alert_dict[message] = "WARN"
        self.alert_level = Alert.WARN

    def error(self, message: str):
        self.alert_dict[message] = "ERROR"
        self.alert_level = Alert.ERROR

    def send_message(self, application: str = 'Default App'):
        if self.alert_level:
            alert_color = 'https://i.groupme.com/472x270.png.9682c71f841c4a878be541cd8cf7cccf'
        elif self.alert_level == Alert.WARN:
            alert_color = 'https://i.groupme.com/600x446.png.44de5706526a41a0ae3038e7714bbcce'
        else:
            alert_color = "https://i.groupme.com/600x446.png.44de5706526a41a0ae3038e7714bbcce"

        post_message = {'bot_id': '888bb6b4dd3bbd5e6e6304db5f', 'text': f'{application}:\n{self.__str__()}',
                        'attachments': [{'type': 'image', 'url': f'{alert_color}'}]}
        print(post_message)

        _ = requests.post('https://api.groupme.com/v3/bots/post', data=post_message)

    def __str__(self):
        alert_str = f"Alerting level: {groupme_annoyance_mapper[self.alert_level]}\n"
        for k, v in self.alert_dict.items():
            alert_str += f"{v}: {k}\n"
        return alert_str


alert = Alerting()


def get_alerter():
    return alert


def info(message: str):
    alert.info(message)


def warn(message: str):
    alert.warn(message)


def error(message: str):
    alert.error(message)


def send_message():
    alert.send_message()


if __name__ == '__main__':
    info('test')
    info('test_a')
    send_message()
