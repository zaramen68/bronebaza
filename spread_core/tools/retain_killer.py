import json
import threading

from spread_core.mqtt.variables import VariableJocket, VariableTRS3, VariableReader
from spread_core.tools.service_launcher import Launcher

topics = [
    # 'Jocket/State/1271/Hardware/AppServer/981/RapidaDali/1856/RapidaDaliDimmer/67142/Groups'
    # 'Jocket/State/+/Hardware/AppServer/+/RapidaDali/+/#',
    # 'Tros3/State/+/#'
    # 'Tros3/State/+/Equipment/+/10'
    '#'
]


class RetainKiller(Launcher):
    _dumped = False
    exit_timer = None

    def __init__(self):
        self._manager = self
        self.restart_timer()
        super(RetainKiller, self).__init__()

    def start(self):
        for topic in topics:
            print('subscribed to {}'.format(topic))
            self.mqttc.subscribe(topic)

    def restart_timer(self):
        if self.exit_timer:
            self.exit_timer.cancel()
        self.exit_timer = threading.Timer(3, function=self.exit_handler, args=[1, None])
        self.exit_timer.start()

    def empty_data_founder(self, topic, data):
        try:
            if 'Jocket' in topic:
                data = make_jocket(data)
            elif 'Equipment' in topic:
                data = VariableTRS3(VariableReader(data))
            else:
                return
            if data.value == '':
                print(f'{topic}')
        except BaseException as ex:
            print(str(ex))
        finally:
            self.restart_timer()

    def retain_killer(self, topic, data):
        try:
            if len(data) > 0:
                print(topic)
                self.mqttc.publish(topic, None, retain=True)
        except BaseException as ex:
            print(str(ex))
        finally:
            self.restart_timer()

    def get_value(self, topic, data):
        if 'Jocket' in topic:
            data = make_jocket(data)
        elif 'Equipment' in topic:
            data = make_tros3(data)
        else:
            return
        print(f'{data.id}: {data.value}')

    def on_message(self, mosq, obj, msg):
        # self.get_value(msg.topic, msg.payload)
        # self.empty_data_founder(msg.topic, msg.payload)
        self.retain_killer(msg.topic, msg.payload)

    def on_exit(self, sig, frame):
        super(RetainKiller, self).on_exit(sig, frame)


def make_jocket(data):
    data = json.loads(data.decode())
    data = VariableJocket(data)
    return data


def make_tros3(data):
    data = VariableTRS3(VariableReader(data))
    return data


if __name__ == '__main__':
    RetainKiller()
