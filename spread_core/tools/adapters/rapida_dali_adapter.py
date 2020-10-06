import logging
import sys

import paho.mqtt.client

from spread_core.mqtt import TopicDali, TopicCan, DUMP, SEND, of, DOUBLE_SEND_FLAG, FORCE_ANSWER_FLAG, DALI_ERROR_FLAG
from spread_core.protocols.dali.bus.can_bus import CanBus
from spread_core.tools import settings, debugger

logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s',
                    level=logging.DEBUG if sys.argv[len(sys.argv) - 1] == 'debug' else logging.INFO,
                    stream=sys.stdout)

responses = dict()


def on_connect(mosq, obj, flags, rc):
    connct_results = ['Connection successful',
                      'Connection refused - incorrect protocol version',
                      'Connection refused - invalid client identifier',
                      'Connection refused - server unavailable',
                      'Connection refused - bad username or password',
                      'Connection refused - not authorised']
    if rc == 0:
        logging.info(connct_results[rc])
        subscribe(TopicDali(SEND))
        subscribe(TopicCan(DUMP))
    else:
        logging.warning(connct_results[rc])


def on_message(mosq, obj, msg):
    topic = of(msg.topic)
    payload = msg.payload.decode()
    if isinstance(topic, TopicCan):
        can2dali(topic, payload)
    elif isinstance(topic, TopicDali):
        dali2can(topic, payload)


def can2dali(from_topic, payload):
    addr, data, flags = payload.split('#')
    data = bytearray.fromhex(data)
    ch_bite = 1 if from_topic.class_id == 1 else 2

    channel = data[ch_bite] - ((data[ch_bite] >> 3) << 3)
    pr, module_id, address_to = parse_address(addr)
    if address_to == 31:
        topic = TopicDali(DUMP, CONTROLLER_ID, module_id, channel)
        message = ''.join(hex(b).replace('0x', '').rjust(2, '0') for b in data[(ch_bite + 1):]).upper() + '#' + flags
        mqttc.publish(str(topic), message)
        logging.debug('[{}]: {}'.format(topic, message))


def on_can_msg(module_id, data):
    # data = ''.join(hex(b).replace('0x', '').rjust(2, '0') for b in data)
    channel = data[0] & 0x07    # 0000111
    dlc = (data[0] >> 3) & 0x03 # 0011000
    err = (data[0] >> 5) & 0x01 # 0100000
    topic = TopicDali(DUMP, CONTROLLER_ID, module_id, channel)
    flags = []
    if dlc == 2:
        flags.append(FORCE_ANSWER_FLAG)
    if err == 1:
        flags.append(DALI_ERROR_FLAG)
    message = ''.join(hex(b).replace('0x', '').rjust(2, '0') for b in data[1:]).upper()
    if len(flags) > 0:
        message += ':'.join(flags)
    mqttc.publish(str(topic), message)
    logging.debug('[{}]: {}'.format(topic, message))


def dali2can(from_topic, payload):
    bite1 = 0x0
    if '#' in payload:
        data, flags = payload.split('#')
        if DOUBLE_SEND_FLAG in flags:
            bite1 += 1 << 0
        # if THREE_BYTES_FLAG in flags:
        if len(data) == 2*3:
            bite1 += 1 << 1
        if FORCE_ANSWER_FLAG in flags:
            bite1 += 1 << 2
    else:
        data = payload

    bite2 = int(str(from_topic.channel_id), 2)

    addr = (31 << 5) + from_topic.module_id
    addr_str = hex(addr).replace('0x', '').rjust(2, '0').rjust(3, '0')
    data = b'\x01' + bytes([bite1, bite2]) + bytes.fromhex(data)
    data_str = '#'.join([addr_str, ''.join(hex(b)[2:].rjust(2, '0') for b in data)])
    can.send(addr, data)
    # to_topic = TopicCan(SEND, CONTROLLER_ID, 0)
    # mqttc.publish(str(to_topic), '{}#{}'.format(addr, data).upper())


def parse_address(addr):
    bin_addr = bin(int(addr, 16))[2:].rjust(11, '0')
    addr_from = int(bin_addr[1:6], 2)
    addr_to = int(bin_addr[6:], 2)
    pr = bin_addr[0]

    return pr, addr_from, addr_to


def subscribe(topic):
    mqttc.subscribe(str(topic))
    logging.debug('Subscribed to {}'.format(topic))


if sys.argv[len(sys.argv) - 1] == 'debug':
    debugger.attach()
try:
    can = CanBus(on_can_msg)
except BaseException as ex:
    logging.error(ex)
else:
    conf = settings.config
    CONTROLLER_ID = conf['CONTROLLER_ID']
    mqttc = paho.mqtt.client.Client(userdata='RapidaDaliAdapter', clean_session=True)
    settings.create_client(mqttc, on_connect=on_connect, on_message=on_message)
    mqttc.loop_forever()
