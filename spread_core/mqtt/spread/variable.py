import json
from datetime import datetime


class Variable:

    def __init__(self, value, flags: list = (), timestamp: datetime = None):
        self._value = value
        self._invalid = value is None
        self._flags = list(flags)

        if isinstance(timestamp, str):
            self._timestamp = datetime.fromisoformat(timestamp)
        elif timestamp is None:
            self._timestamp = datetime.now()
        else:
            self._timestamp = timestamp

    value = property(fget=lambda self: self._value, doc='value')
    timestamp = property(fget=lambda self: self._timestamp, doc='timestamp')
    invalid = property(fget=lambda self: self._invalid, doc='invalid')
    flags = property(fget=lambda self: self._flags, doc='flags')

    def __str__(self):
        r = f'{str(self._value).ljust(11, " ")} ({str(self._timestamp)})'
        if self._flags:
            r += f' {";".join(self._flags)}'
        return r

    def __iter__(self):
        data = dict(timestamp=self._timestamp.astimezone().isoformat())

        if not self._invalid:
            data['value'] = self._value
        else:
            data['invalid'] = True

        for flag in self._flags:
            data[flag] = True
        return data

    def pack(self):
        return json.dumps(self.__iter__())
