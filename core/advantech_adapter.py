import socket
import threading
import time
from threading import Timer

from spread_core.mqtt.variables import VariableTRS3
from spread_core.tools import settings
from spread_core.tools.service_launcher import Launcher
from spread_core.tools.settings import config, logging

settings.DUMPED = False
PROJECT = config['PROJ']
BUS_ID = config['BUS_ID']
HOST = config['BUS_HOST']
PORT = config['BUS_PORT']
HOSTnPORT = config['BUS_HOST_PORT']
TIMEOUT = config['BUS_TIMEOUT']
KILL_TIMEOUT = config['KILL_TIMEOUT']
THINGS=config['THINGS']

topic_dump = 'Tros3/State/{}/{}/{}'
topic_send = 'ModBus/from_Client/{}'
topic_dali = 'Jocket/Command/{projet_id}/le_sid/Hardware/AppServer/{server_id}/RapidaDali/{}manager_id/RapidaDaliDimmer/{provider_id}/BrightnessLevel'
is_night = False


class ModbusTcpSocket:

    def __init__(self, host, port, commands):

        self._killer = None
        self._port=port
        self._host=host
        self.sock=None
        #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock.settimeout(TIMEOUT)
        self._commands=commands



    def create(self):
        logging.debug('Create socket')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(TIMEOUT)
        while True:
            try:
                self.sock.connect((self._host, self._port))
            except ConnectionRefusedError as ex:
                logging.exception(ex)
                time.sleep(3)
            else:
                break

    def start_timer(self):
        if KILL_TIMEOUT > 0:
            self._killer = Timer(KILL_TIMEOUT, self.kill)
            self._killer.start()

    def stop_timer(self):
        if self._killer:
            self._killer.cancel()
            self._killer = None

    def kill(self):
        if isinstance(self.sock, socket.socket):
            logging.debug('Kill socket')
            self.sock.close()
            self.sock = None

    def send_message(self, data, r_size):
        self.stop_timer()
        if self.sock is None:
            self.create()
        #out = b''
        self.sock.send(data)
        logging.debug('[->  ]: {}'.format(' '.join(hex(b)[2:].rjust(2, '0').upper() for b in data)))
       # while len(out) < r_size:
       #     out += self.sock.recv(1024)
       #if len(out) > r_size:
       #     out = out[out.rindex(data[0]):]
        out=self.sock.recv(2048)
        logging.debug('[  <-]: {}'.format(' '.join(hex(b)[2:].rjust(2, '0').upper() for b in out)))
        out_str='{}'.format(''.join(hex(b)[2:].rjust(2, '0').upper() for b in out))
        return out_str

    def commands(self):
        return self._commands


class ModBusTCPAdapterLauncher(Launcher):
    _dumped = False
    _command_event = threading.Event()

    def __init__(self):
        self._manager = self
        self._stopped = False
        self.sock=[]

        for host, port, commands in HOSTnPORT:
            self.sock.append(ModbusTcpSocket(host, port, commands ))
        super(ModBusTCPAdapterLauncher, self).__init__()


    def start(self):
        self._command_event.set()
        listen = threading.Thread(target=self.listen_all)
        listen.start()

        self.mqttc.subscribe(topic_send.format(BUS_ID))
        logging.debug('Subscribed to {}'.format(topic_send.format(BUS_ID)))



    def on_message(self, mosq, obj, msg):

        self._command_event.clear()
        self._stopped = True
        global is_night


        host, port, data, flags = msg.payload.decode().split('#')

        data = bytes.fromhex(data)
        flags = flags.split(':')
        if flags == 1:
            is_night = False
        if flags == 0:
            is_night = True


        self._stopped = False
        self._command_event.set()
       # self.mqttc.subscribe(topic_send.format(BUS_ID))
        #self.mqttc.loop_start()



    def mqtt_listen_fun(self):
        self.mqttc.subscribe(topic_send.format(BUS_ID))
     #   self.mqttc.loop_forever()
        logging.debug('Subscribed to {}'.format(topic_send.format(BUS_ID)))

    def write_to_bro(self, topId, num, value):
        out = VariableTRS3(None, topId, num, value)
        self.mqttc.publish(topic=topic_dump.format(PROJECT, str(topId), str(num)), payload=out.pack())
        logging.debug('[  <-]: {}'.format(out))

    def listen_all(self):


        while True:
            time.sleep(1)
            self._command_event.wait()



            for device in self.sock:
                #for data in device.commands():
                #    size=len(data)
                #    data = bytes.fromhex(data)
                 #   try:
                        #tk=2415
                #        out = device.send_message(data, size)
                        #print(data)
                #    except BaseException as ex:
                #        logging.exception(ex)
                #        self.mqttc.publish(topic=topic_dump.format(BUS_ID) + '/error', payload=str(ex))
                 #   else:
                        for thing in THINGS:
                                num=0
                                for key, value in thing['topicValues'].items():

                                    if (key == 'isOpenedId') and (thing['di'] != 'None'):
                                        data = thing['command']
                                        size = len(data)
                                        data = bytes.fromhex(data)
                                        try:
                                            # tk=2415
                                            out = device.send_message(data, size)
                                        except BaseException as ex:
                                            logging.exception(ex)
                                            self.mqttc.publish(topic=topic_dump.format(BUS_ID) + '/error',
                                                               payload=str(ex))
                                        else:
                                            #value = self.get_from_DI(thing['id'], out)
                                            tt = out[18:22]
                                            tk = int(tt, 16)
                                            if tk == 1:
                                                # Дверь открыта

                                                value='True'
                                                #  Запомнить состояние освещения
                                                #     погасить свет
                                                #
                                                #

                                            if tk == 0:
                                                value='False'

                                            #value = VariableTRS3(None, int(BUS_ID), 0, tk)
                                            #top_out = topic_dump.format(PROJECT, BUS_ID, '0')
                                            #self.mqttc.publish(topic=topic_dump.format(PROJECT, BUS_ID, '0'),
                                            #                   payload=out.pack())
                                            #logging.debug('[  <-]: {}'.format(out))

                                            self.write_to_bro(thing['topicId'], num, value)
                                    else:
                                        self.write_to_bro(thing['topicId'], num, value)
                                        num = num+1





        #    self._step_event.clear()



def run():
    ModBusTCPAdapterLauncher()




if __name__ == '__main__':
    run()
    # TCPAdapterLauncher()