import signal
import traceback
from datetime import datetime

import paho.mqtt.client

from spread_core.tools import settings, debugger
from spread_core.tools.settings import *

"""DEBUGGING"""
if DEBUGGING_HOST in config:
    debugger.attach()


"""ABSTRACT LAUNCHER"""


class Launcher:
    _dumped = True
    _mqttc = None
    _manager = None

    def __init__(self):
        if self._dumped:
            if DUMP in config:
                generate_dump()
            else:
                raise BaseException('DUMP file is not provided! Break!')
        signal.signal(signal.SIGTERM, self.exit_handler)
        if sys.platform == "linux" or sys.platform == "linux2":
            signal.signal(signal.SIGQUIT, self.exit_handler)
        signal.signal(signal.SIGINT, self.exit_handler)
        self.connect()

    @property
    def mqttc(self):
        if self._mqttc is None:
            self._mqttc = paho.mqtt.client.Client(userdata=self.__class__.__name__, clean_session=True)
        return self._mqttc

    def connect(self):
        self.create_client(self.mqttc,
                           on_connect=self.on_connect,
                           on_publish=self.on_publish,
                           on_subscribe=self.on_subscribe,
                           on_message=self.on_message,
                           on_log=self.on_log,
                           on_unsubscribe=self.on_unsubscribe,
                           on_disconnect=self.on_disconnect)
        self.mqttc.loop_forever()

    def on_connect(self, mosq, obj, flags, rc):
        connect_results = ['Connection successful',
                           'Connection refused - incorrect protocol version',
                           'Connection refused - invalid client identifier',
                           'Connection refused - server unavailable',
                           'Connection refused - bad username or password',
                           'Connection refused - not authorised']
        if rc < len(connect_results):
            logging.info(connect_results[rc])
            if rc == 0:
                if self._manager is not None and hasattr(self._manager, 'start'):
                    try:
                        self._manager.start()
                    except BaseException as ex:
                        logging.exception(ex)
                else:
                    logging.warning('{} does not have a "start()" method. Break!'.format(self.__class__.__name__))
        else:
            logging.warning("rc: " + str(rc))

    def subscribe(self, topic):
        self.mqttc.subscribe(str(topic))
        logging.debug('Subscribed to {}'.format(topic))

    def unsubscribe(self, topic):
        self.mqttc.unsubscribe(str(topic))
        logging.debug('Unsubscribe from {}'.format(topic))

    def publish(self, topic, data, retain=False):
        logging.debug(f'{"R" if retain else " "} [{topic}]: '.ljust(100, '.') + f' {data}')
        _data = data if isinstance(data, str) else data.pack()
        self.mqttc.publish(str(topic), _data, retain=retain)

    def on_exit(self, sig, frame):
        settings.on_exit()
        if self.mqttc:
            try:
                self.mqttc.disconnect()
                self.mqttc.loop_stop(True)
            except BaseException as ex:
                logging.error(ex)

    def exit_handler(self, sig, frame):
        logging.info('signal({}) received!'.format(sig))
        try:
            self.on_exit(sig, frame)
        except BaseException as ex:
            logging.exception(ex)

        logging.info('signal({}) handled!'.format(sig))
        sys.exit(0)

    """EXTENSION FUNCTIONS"""
    def on_log(self, client, userdata, level, buffer): pass
    def on_publish(self, client, userdata, mid): pass
    def on_message(self, mosq, obj, msg): pass
    def on_unsubscribe(self, client, userdata, mid): pass
    def on_subscribe(self, client, userdata, mid): pass
    def on_disconnect(self, client, userdata, rc): pass
    """"""

    @staticmethod
    def create_client(mqttc, on_connect=None, on_message=None, on_subscribe=None, on_publish=None, on_close=None,
                      on_log=None, on_unsubscribe=None, on_disconnect=None, exit_handler=None):
        if BROKER_USERNAME in config and BROKER_PASSWORD in config:
            mqttc.username_pw_set(config[BROKER_USERNAME], config[BROKER_PASSWORD])
        global _mqttc, _exit_handler
        _mqttc = mqttc
        _exit_handler = exit_handler
        mqttc.on_connect = on_connect
        mqttc.on_message = on_message
        mqttc.on_subscribe = on_subscribe
        mqttc.on_publish = on_publish
        mqttc.on_socket_close = on_close
        mqttc.on_log = on_log
        mqttc.on_unsubscribe = on_unsubscribe
        mqttc.on_disconnect = on_disconnect
        host = config[BROKER_HOST]
        port = config[BROKER_PORT]
        logging.info('Connect to {}:{}'.format(host, port))
        mqttc.connect(host=host, port=port)

    @staticmethod
    def log_uncaught_exceptions(ex_cls, ex, tb):
        text = '{}: {} {}:\n'.format(datetime.now(), ex_cls.__name__, ex)
        text += ''.join(traceback.format_tb(tb))
        logging.error(text)


"""FORCE EXCEPTION LOGGING"""
sys.excepthook = Launcher.log_uncaught_exceptions
