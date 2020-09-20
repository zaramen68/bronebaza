from spread_core.tools.settings import logging


class ManagerOfBroker:
    def __init__(self, mqttc, use_retain):
        self.mqttc = mqttc
        self.use_retain = use_retain

    def subscribe(self, topic, log=True):
        self.mqttc.subscribe(str(topic))
        if log:
            logging.info('Subscribed to {}'.format(topic))

    def unsubscribe(self, topic, log=True):
        self.mqttc.unsubscribe(str(topic))
        if log:
            logging.info('Unsubscribe from {}'.format(topic))

    def publish(self, topic, data, retain=False):
        logging.debug('[{}]: r: {}; data: {}'.format(topic, retain, data))
        _data = data if isinstance(data, str) else data.pack()
        self.mqttc.publish(str(topic), _data, retain=self.use_retain and retain)

    def publish_retain(self, topic, data, retain=False):
        logging.debug('[{}]: r: {}; data: {}'.format(topic, retain, data))
        _data = data if isinstance(data, str) else data.pack()
        self.mqttc.publish(str(topic), _data, retain=retain)
