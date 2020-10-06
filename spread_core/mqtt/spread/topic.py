import spread_core.mqtt.spread.address as address
from spread_core.errors.project_errors import AddressError, TopicError


class SpreadTopic:
    protocol = 'Spread'
    action = None

    def __init__(self, entity_address: address.EntityAddress):
        self.entity_address = entity_address

    def __str__(self):
        return '/'.join(str(i) for i in [self.protocol, self.action, self.entity_address])

    def __repr__(self):
        return self.__str__()


class TopicProject:
    protocol = 'Project'
    action = 'File'

    def __init__(self, p_id, file_name):
        self.project_id = p_id
        self.file_name = file_name

    def __str__(self):
        return '/'.join(str(i) for i in [self.protocol, self.action, self.project_id, self.file_name])


class State(SpreadTopic):
    action = 'State'


class Event(SpreadTopic):
    action = 'Event'


class Renew(SpreadTopic):
    action = 'Renew'


class Set(SpreadTopic):
    action = 'Set'


def topic_of(string: str):
    try:
        if string.endswith('/'):
            string = string[:-1]
        arr = string.split('/')
        if arr[0] == TopicProject.protocol:
            return TopicProject(*arr[-2:])
        else:
            action = arr[1]
            for scl in SpreadTopic.__subclasses__():
                if issubclass(scl, SpreadTopic) and scl.action == action:
                    entity_address = address.of('/'.join(arr[2:]))
                    return scl(entity_address)

        raise
    except AddressError as ex:
        raise ex
    except BaseException:
        raise TopicError(string)
