import base64
import datetime
import json
import threading
import zlib

from spread_core.bam import engineries, generator
from spread_core.bam.const import *
from spread_core.errors.project_errors import ProjectError
from spread_core.mqtt import *
from spread_core.mqtt.variables import VariableTRS3, VariableReader
from spread_core.tools.service_launcher import Launcher
from spread_core.tools.settings import logging, PROJECT_ID, config


class Statistic(Launcher):
    key = '{00000000-0000-0000-0000-000000000000}'
    _dumped = False

    def __init__(self):
        self._manager = self
        self._engineries = []
        self.ids = []
        self.id = config[PROJECT_ID]
        self.timer = None
        self.cur_e = None
        super(Statistic, self).__init__()

    def start(self):
        self.subscribe(TopicProject(self.id, f'{ENGINERIES}.json'))

    def on_project(self, data):
        data = json.loads(data)
        if ENGINERIES in data:
            try:
                self.unsubscribe(TopicProject(self.id, f'{ENGINERIES}.json'))
                for e_data in data[ENGINERIES]:
                    if RECIPE in e_data and TYPE in e_data[RECIPE] and INGREDIENTS in e_data[RECIPE]:
                        if e_data[RECIPE][TYPE] == 'Simple' and len(e_data[RECIPE][INGREDIENTS]) > 0:
                            if e_data[TYPE] == engineries.DimmingLight.__name__:
                                try:
                                    _eng = generator.generate_enginery(self.id, e_data, None)
                                    self._engineries.append(_eng)
                                    self.ids.append(_eng.id)
                                except ProjectError as ex:
                                    logging.warning(ex)
            except BaseException as ex:
                logging.exception(ex)
            finally:
                self.on_ready()

    def start_timer(self):
        self.stop_timer()
        self.timer = threading.Timer(5, self.on_time_out)
        self.timer.start()

    def stop_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None

    def on_time_out(self):
        logging.info(f'{self.cur_e}: unknown')

    def on_ready(self):
        logging.info('Project({}) loaded'.format(self.id))

        class_id = engineries.DimmingLight._cmds.index(engineries.historyResponseId)
        topic = f'Tros3/Reply/{self.id}/{self.key}/+/{class_id}'
        self.subscribe(topic)
        self.req()

    def req(self):
        if len(self._engineries) > 0:
            self.cur_e = self._engineries.pop(0)
            class_id = engineries.DimmingLight._cmds.index(engineries.historyRequestId)
            topic = TopicCommandTros3(
                self.id,
                entity_addr=EntityAddress('/'.join([self.key, str(self.cur_e.id), str(class_id)]))
            )

            data = {
                "algorithm": "raw",
                "startTime": "2020-08-01T00:00:00",
                "endTime": "2020-09-01T00:00:00",
                "key": f'{self.key}'
            }

            var = VariableTRS3(id=self.cur_e.id, cl=class_id, val=json.dumps(data))
            try:
                self.start_timer()
                self.publish(topic, var)
            except BaseException as ex:
                logging.exception(ex)
        else:
            self.on_exit(0, None)

    def on_reply(self, variable):
        if variable.id in self.ids:
            try:
                if self.cur_e and variable.id == self.cur_e.id:
                    self.stop_timer()
                response = json.loads(variable.value)
                static = base64.b64decode(response['data'])
                static = zlib.decompress(static).decode()
                static = static.split()

                time_from = None
                period = 0
                while len(static) > 0:
                    try:
                        item = static.pop(0).split(',')
                        time = datetime.datetime.fromisoformat(item[1].replace('"', ''))
                        valid = bool(int(item[2]))
                        if valid:
                            value = int(item[3])

                            if time_from:
                                gap = int(time.timestamp() - time_from.timestamp())
                                period += gap

                            if value > 0:
                                time_from = time
                            else:
                                time_from = None

                    except BaseException as ex:
                        logging.exception(ex)

            except BaseException as ex:
                logging.exception(ex)
            else:
                logging.info(f'{variable.id}: {int(period/60/60 * 10)/10}')

            self.req()

    def on_message(self, mosq, obj, msg):
        if msg.topic.startswith(f'Tros3/Reply/'):
            var = VariableTRS3(VariableReader(msg.payload))
            self.on_reply(var)
        else:
            topic = mqtt.of(msg.topic)
            if isinstance(topic, mqtt.TopicProject):
                self.on_project(msg.payload.decode())


if __name__ == '__main__':
    Statistic()
