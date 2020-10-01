import inspect

from spread_core.errors.project_errors import AddressError


class EntityAddress:
    family = None

    def __init__(self, project_id: int, e_type: str, e_id: int, funit_type: str):
        self._project_id = project_id
        self._type = e_type
        self._id = e_id
        self._funit_type = funit_type

    project_id = property(fget=lambda self: try_get_int(self._project_id), doc='project_id')
    type = property(fget=lambda self: self._type, doc='type')
    id = property(fget=lambda self: try_get_int(self._id), doc='id')
    funit_type = property(fget=lambda self: self._funit_type, doc='funit_type')

    def __str__(self):
        return '/'.join(str(i) for i in [self.project_id, self.family, self.type, self.id, self.funit_type])

    def __repr__(self):
        return self.__str__()


class BroadcastAddress(EntityAddress):
    family = '#'

    def __init__(self, project_id: int):
        super(BroadcastAddress, self).__init__(project_id, '', 0, '')

    def __str__(self): return '/'.join(str(i) for i in [self.project_id, self.family])


class ManagerAddress(EntityAddress):
    family = 'Hardware'

    @staticmethod
    def of(manager, funit_type='#'):
        return ManagerAddress(manager.project_id, manager.__class__.__name__, manager.id, funit_type)


class ProviderAddress(EntityAddress):
    family = 'Hardware'

    def __init__(self, project_id, manager_type, manager_id, e_type, e_id, funit_type):
        super(ProviderAddress, self).__init__(project_id, e_type, e_id, funit_type)
        self._manager_type = manager_type
        self._manager_id = manager_id

    manager_type = property(fget=lambda self: self._manager_type, doc='manager_type')
    manager_id = property(fget=lambda self: try_get_int(self._manager_id), doc='manager_id')

    def __str__(self):
        return '/'.join(
            str(i) for i in [self.project_id, self.family, self.manager_type, self.manager_id, self.type, self.id, self.funit_type])

    @staticmethod
    def of(provider, funit_type='#'):
        return ProviderAddress(provider.manager.project_id,
                               provider.manager.__class__.__name__,
                               provider.manager.id,
                               provider.__class__.__name__,
                               provider.id,
                               funit_type)


class EngineryAddress(EntityAddress):
    family = 'Equipment'


class SubgineryAddress(EntityAddress):
    family = 'Location'

    def __init__(self, project_id, location_id: int, subginery_type: str, funit_type):
        super(SubgineryAddress, self).__init__(project_id, subginery_type, location_id, funit_type)

    def __str__(self):
        return '/'.join(str(i) for i in [self.project_id, self.family, self.id, self.type, self.funit_type])


def of(string: str):
    arr = string.split('/')
    try:
        family = arr.pop(1)
        for cl in EntityAddress.__subclasses__():
            if issubclass(cl, EntityAddress) and cl.family == family and len(inspect.getfullargspec(cl).args) - 1 == len(arr):
                return cl(*arr)

        raise
    except BaseException:
        raise AddressError(string)


def try_get_int(obj):
    if str(obj).isdigit():
        return int(obj)
    return str(obj)
