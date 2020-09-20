import logging
import struct
import sys
import traceback
from datetime import datetime
from threading import Thread

import can
import paho.mqtt.client

from spread_core.tools import settings, debugger


def log_uncaught_exceptions(ex_cls, ex, tb):
    text = '{}: {}:\n'.format(ex_cls.__name__, ex)
    text += ''.join(traceback.format_tb(tb))
    logging.error(text)


logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s',
                    level=logging.INFO,
                    stream=sys.stdout)

sys.excepthook = log_uncaught_exceptions

bus = None
topic_dump = 'Bus/Dump/Rapida/{}/Can/{}/{}'
topic_send = 'Bus/Send/Rapida/+/Can/0'


def on_connect(mosq, obj, flags, rc):
    connct_results = ['Connection successful',
                      'Connection refused - incorrect protocol version',
                      'Connection refused - invalid client identifier',
                      'Connection refused - server unavailable',
                      'Connection refused - bad username or password',
                      'Connection refused - not authorised']
    if rc == 0:
        logging.info(connct_results[rc])
        global bus
        try:
            bus = can.Bus(channel='can0', bustype='socketcan')
            thr = Thread(target=listen_can, args=[bus])
            thr.setDaemon(True)
            thr.start()
        except BaseException as ex:
            logging.exception(ex)
        mqttc.subscribe(topic_send)
    else:
        logging.warning(connct_results[rc])


def on_message(mosq, obj, msg):
    try:
        arr = msg.payload.decode().split('#')
        addr = arr[0]
        addr = addr.rjust(4, '0')
        addr = ''.join(addr[i:i+2] for i in range(0, len(addr), 2))
        addr = struct.unpack('>h', bytes.fromhex(addr))[0]

        data = arr[1]
        if data[-2:] == ' 0':
            data = data[:-2]
        data.replace('.', '')
        data = bytes.fromhex(data)

        message = can.Message(arbitration_id=addr, data=data, timestamp=datetime.now().timestamp(), extended_id=False)
        if bus:
            bus.send(message)
        else:
            logging.warning(str.format('SEND failed: {}', message))
    except BaseException as ex:
        logging.exception(ex)


def listen_can(bus):
    while True:
        try:
            for msg in bus:
                addr = hex(msg.arbitration_id).replace('0x', '').rjust(3, '0')
                from_addr = msg.arbitration_id >> 5
                if from_addr in range(1, 31):
                    data = ''.join(hex(b).replace('0x', '').rjust(2, '0') for b in msg.data)
                    bite0 = data[0:2]
                    topic = topic_dump.format(CONTROLLER_ID, CAN_ID, bite0)
                    message = '{}#{}'.format(addr, data).upper()
                    mqttc.publish(topic, message)
                    logging.info('DUMP [{}]: {}'.format(topic, message))
        except BaseException as ex:
            logging.exception(ex)


conf = settings.config
CONTROLLER_ID = conf['CONTROLLER_ID']
CAN_ID = conf['CAN_ID']

if sys.argv[len(sys.argv) - 1] == 'debug':
    debugger.attach()

mqttc = paho.mqtt.client.Client(userdata='CanAdapter', clean_session=True)
settings.create_client(mqttc, on_connect=on_connect, on_message=on_message)
mqttc.loop_forever()
