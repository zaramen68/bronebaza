from spread_core import mqtt
from spread_core.errors.project_errors import ClassifierError, AddressError


class TopicData:
    def __init__(self, branch, type, p_id):
        self.branch = branch
        self.type = type
        self.p_id = int(p_id)
        self._topic = None

    def get_parts(self):
        return [str(self.branch), str(self.type), str(self.p_id)]

    def __str__(self):
        return '/'.join(self.get_parts())

    def __repr__(self):
        return self.__str__()


class TopicReply(TopicData):
    def __init__(self, p_id, session_id):
        super().__init__(mqtt.JOCKET, mqtt.REPLY, p_id)
        self.session_id = session_id

    def get_parts(self):
        return super().get_parts() + [str(self.session_id)]


class TopicCommand(TopicData):
    def __init__(self, p_id, session_id='+', entity_addr='#'):
        super().__init__(mqtt.JOCKET, mqtt.COMMAND, p_id)
        self.session_id = session_id
        self.entity_addr = entity_addr

    def get_parts(self):
        return super().get_parts() + [self.session_id] + (self.entity_addr.get_parts() if isinstance(self.entity_addr, EntityAddress) else [self.entity_addr])


class TopicCommandTros3:
    def __init__(self, p_id, entity_addr='#'):
        self.p_id = p_id
        self.entity_addr = entity_addr

    def __str__(self):
        res = [mqtt.TROS3, mqtt.COMMAND, str(self.p_id)]
        if isinstance(self.entity_addr, EntityAddress):
            res += self.entity_addr.get_parts()
        else:
            res.append('#')
        return '/'.join(res)


class TopicState(TopicData):
    def __init__(self, p_id, entity_addr='#'):
        super().__init__(mqtt.JOCKET, mqtt.STATE, p_id)
        self.entity_addr = entity_addr

    def get_parts(self):
        return super().get_parts() + (self.entity_addr.get_parts() if isinstance(self.entity_addr, EntityAddress) else [self.entity_addr])


class TopicStateTros3(TopicData):
    def __init__(self, p_id, entity_addr='#'):
        super().__init__(mqtt.TROS3, mqtt.STATE, p_id)
        self.entity_addr = entity_addr

    def get_parts(self):
        return super().get_parts() + (self.entity_addr.get_parts() if isinstance(self.entity_addr, EntityAddress) else [self.entity_addr])


class TopicProject(TopicData):
    def __init__(self, p_id, file_name):
        super().__init__(mqtt.PROJECT, mqtt.FILE, p_id)
        self.file_name = file_name

    def get_parts(self):
        return super().get_parts() + [str(self.file_name)]


class EntityAddress:
    def __init__(self, entity_type):
        self.entity_type = entity_type

    def __str__(self):
        return '/'.join(self.get_parts())

    def get_parts(self):
        return [str(self.entity_type)]

    def entity_id(self):
        raise NotImplementedError()

    @staticmethod
    def of(*args):
        if args[0] == mqtt.HARDWARE:
            if len(args) == 4:
                return ServerAddress(*args[1:])
            elif len(args) == 6:
                return ManagerAddress(*args[1:])
            elif len(args) == 8:
                return ProviderAddress(*args[1:])
        else:
            return EngineryAddress(*args[-2:])

        raise AddressError('/'.join(args))


class ServerAddress(EntityAddress):
    def __init__(self, server_type='+', server_id='+', is_sharp=True):
        super().__init__(mqtt.HARDWARE)
        self.server_type = server_type
        self.server_id = server_id
        self.is_server_sharp = is_sharp

    def get_parts(self):
        if self.is_server_sharp:
            return super().get_parts() + [str(self.server_type), str(self.server_id)] + ['#']
        else:
            return super().get_parts() + [str(self.server_type), str(self.server_id)]

    @property
    def entity_id(self):
        return self.server_id


class ManagerAddress(ServerAddress):
    @staticmethod
    def of(entity, funit_type=True):
        return ManagerAddress(mqtt.S_TYPE, entity._server_id, entity.__class__.__name__, entity.id, funit_type)

    def __init__(self, server_type=str, server_id=int, manager_type=str, manager_id=int, is_sharp=True):
        super().__init__(server_type, server_id, False)
        self.funit = None
        self.funit_type = None
        self.manager_type = manager_type
        self.manager_id = manager_id
        if isinstance(is_sharp, bool):
            self.is_manager_sharp = is_sharp
        else:
            self.is_manager_sharp = False
            self.set_funit_type(is_sharp)

    @property
    def entity_id(self):
        return self.manager_id

    def set_funit_type(self, funit_type):
        if funit_type == '+':
            return
        if self.manager_type == '+':
            return
        self.funit_type = funit_type
        if self.manager_type in mqtt.classifier:
            if funit_type in mqtt.classifier[self.manager_type]:
                self.funit = mqtt.classifier[self.manager_type][funit_type]
            else:
                raise ClassifierError('Объект {} отсутствует в classifier[{}]'.format(funit_type, self.manager_type))
        else:
            raise ClassifierError('Объект {} отсутствует в classifier'.format(self.manager_type))

    def get_parts(self):
        if self.is_manager_sharp:
            return super().get_parts() + [str(self.manager_type), str(self.manager_id)] + ['#']
        elif isinstance(self, ProviderAddress):
            return super().get_parts() + [str(self.manager_type), str(self.manager_id)]
        else:
            return super().get_parts() + [str(self.manager_type), str(self.manager_id), str(self.funit_type)]


class ProviderAddress(ManagerAddress):
    def __init__(self, server_type='+', server_id='+', manager_type='+', manager_id='+', provider_type='+', provider_id='+', funit_type='+'):
        super().__init__(server_type, server_id, manager_type, manager_id, False)
        self.funit = None
        self.funit_type = None
        self.provider_type = provider_type
        self.provider_id = provider_id
        self.set_funit_type(funit_type)

    @property
    def entity_id(self):
        return self.provider_id

    def set_funit_type(self, funit_type):
        self.funit_type = funit_type
        if funit_type == '+':
            return
        if self.manager_type == '+':
            return
        if self.provider_type == '+':
            return
        if self.manager_type in mqtt.classifier:
            if self.provider_type in mqtt.classifier[self.manager_type]:
                if funit_type in mqtt.classifier[self.manager_type][self.provider_type]:
                    self.funit = mqtt.classifier[self.manager_type][self.provider_type][funit_type]
                else:
                    raise ClassifierError('Объект {} отсутствует в classifier[{}][{}]'.format(funit_type, self.manager_type, self.provider_type))
            else:
                raise ClassifierError('Объект {} отсутствует в classifier[{}]'.format(self.provider_type, self.manager_type))
        else:
            raise ClassifierError('Объект {} отсутствует в classifier'.format(self.manager_type))

    def get_parts(self):
        return super().get_parts() + [str(self.provider_type), str(self.provider_id), self.funit_type]

    @staticmethod
    def of(entity, funit_type='#'):
        return ProviderAddress(mqtt.S_TYPE,
                               entity._manager._server_id,
                               entity._manager.__class__.__name__,
                               entity._manager.id,
                               entity.__class__.__name__,
                               entity.id,
                               funit_type)


class SubgineryAddress(EntityAddress):
    def __init__(self, subginery_id='+', funit_id='+'):
        super().__init__(mqtt.EQUIPMENT)
        self.subginery_id = subginery_id
        self.funit_id = funit_id

    def get_parts(self):
        # return super().get_parts() + [str(self.subginery_id), str(self.funit_id)]
        return [str(self.subginery_id), str(self.funit_id)]

    @property
    def entity_id(self):
        return self.subginery_id


class EngineryAddress(EntityAddress):
    def __init__(self, enginery_id='+', funit_id='+'):
        super().__init__(mqtt.EQUIPMENT)
        self.enginery_id = enginery_id
        self.funit_id = funit_id

    @property
    def entity_id(self):
        return self.enginery_id

    def get_parts(self):
        # return super().get_parts() + [str(self.enginery_id), str(self.funit_id)]
        return [str(self.enginery_id), str(self.funit_id)]

    @staticmethod
    def of(enginery, funit_id):
        return EngineryAddress(enginery.id, funit_id)
