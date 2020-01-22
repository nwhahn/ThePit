import requests
from enum import Enum
import logging


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
        self.name = "Default app"
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

    def set_name(self, name: str):
        self.name = name

    def send_message(self):
        if self.alert_level:
            alert_color = 'https://i.groupme.com/472x270.png.9682c71f841c4a878be541cd8cf7cccf'
        elif self.alert_level == Alert.WARN:
            alert_color = 'https://i.groupme.com/600x446.png.44de5706526a41a0ae3038e7714bbcce'
        else:
            alert_color = "https://i.groupme.com/600x446.png.44de5706526a41a0ae3038e7714bbcce"

        post_message = {'bot_id': '888bb6b4dd3bbd5e6e6304db5f', 'text': f'{self.name}:\n{self.__str__()}',
                        'attachments': [{'type': 'image', 'url': f'{alert_color}'}]}
        logging.info(post_message)

        _ = requests.post('https://api.groupme.com/v3/bots/post', data=post_message)

    def __str__(self):
        alert_str = f"Alerting level: {groupme_annoyance_mapper[self.alert_level]}\n"
        for k, v in self.alert_dict.items():
            alert_str += f"{v}: {k}\n"
        if self.alert_level == Alert.ERROR:
            alert_str = f":X::X::X:\n{alert_str}:X::X::X:"
        return alert_str


alert = Alerting()


def get_alerter(name: str):
    alert.set_name(name)
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
