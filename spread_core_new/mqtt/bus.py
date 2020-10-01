from spread_core import mqtt


class TopicBus:
    def __init__(self, direct, controller_id, type):
        self.direct = direct
        self.controller_id = controller_id
        self.type = type

    def is_dump(self):
        return self.direct == mqtt.DUMP

    def is_send(self):
        return self.direct == mqtt.SEND

    def get_parts(self, args=[]):
        return [str(mqtt.BUS), str(self.direct), str(mqtt.RAPIDA), str(self.controller_id), str(self.type)]

    def __str__(self):
        return '/'.join(self.get_parts())

    def __repr__(self):
        return str(self)


class TopicCan(TopicBus):
    def __init__(self, direct, controller_id='+', can_id='+', class_id='+'):
        super().__init__(direct, controller_id, mqtt.CAN)
        self.can_id = can_id
        self.class_id = class_id

    def get_parts(self, args=[]):
        if len(args) == 1:
            return super().get_parts() + args
        if self.direct == mqtt.SEND:
            return super().get_parts() + [str(self.can_id)]
        else:
            return super().get_parts() + [str(self.can_id), str(self.class_id)]


class TopicDali(TopicBus):
    def __init__(self, direct, controller_id='+', module_id='+', channel_id='+'):
        super().__init__(direct, controller_id, mqtt.DALI)
        self.module_id = module_id
        self.channel_id = channel_id

    def get_parts(self, args=[]):
        if len(args) == 2:
            return super().get_parts() + args
        return super().get_parts() + [str(self.module_id), str(self.channel_id)]


class TopicModule(TopicBus):
    def __init__(self, direct, controller_id, type):
        super().__init__(direct, controller_id, type)


class TopicTcp(TopicBus):
    def __init__(self, direct, bus_id='#'):
        super().__init__(direct, bus_id, mqtt.MERCURY)

    def get_parts(self, args=[]):
        return [str(mqtt.BUS), str(self.direct), str(self.type), str(self.controller_id)]


class TopicTcpError(TopicTcp):
    pass
