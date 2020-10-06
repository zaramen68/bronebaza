import json
import struct

from datetime import datetime

ADDR = 'address'
ID = 'id'
TYPE = 'type'
NAME = 'name'
LABEL = 'label'
MANAGER_ID = 'managerID'
LOCATION_ID = 'locationID'
ENGINERIES = 'engineries'
CLASS = 'class'
TIMESTAMP = 'timestamp'
FLAGS = 'flags'
INVALID = 'invalid'
VALUE = 'value'
KEY = 'key'
DATA = 'data'
ERROR = 'error'
CODE = 'code'
MESS = 'message'
ITEMS = 'items'
ACTION = 'action'
SET = 'set'
GET = 'get'
FUNC = 'func'
DELAY = 'delay'
EXPECT = 'expect'
CONDITION = 'condition'
SLEEP = 'sleep'
STATE = 'state'
ATTRIBUTES = 'attributes'
DEVICE = 'device'
BINDING = 'binding'
GROUP = 'group'
EXPEL = 'expel'
RECIPE = 'recipe'
INGREDIENTS = 'ingredients'
CONSTS = 'constants'


class Variable:
    id = None
    cl = None
    value = None
    timeStamp = None
    invalid = False
    key = None

    def __init__(self, id, cl, val):
        self.id = id
        self.cl = cl
        self.value = val

    def __str__(self):
        return str.format('class: {}, id: {}, invalid: {}, value: {}, ' + ' '*(5-len(str(self.value))) + 'timeStamp: {}',
                          self.cl, self.id, self.invalid, self.value, self.timeStamp)

    def __repr__(self):
        return str(self)

    def pack(self):
        pass


class VariableJocket(Variable):
    key = None
    action = None

    def __init__(self, data):
        super().__init__(0, 0, None)
        self.data = data
        self.id = data[ADDR][ID]
        self.cl = data[ADDR][CLASS] if CLASS in data[ADDR] else data[ADDR][CLASS[:-1]]
        if TIMESTAMP in data:
            self.timeStamp = DateTimeJocket(data[TIMESTAMP])
        if KEY in data:
            self.key = data[KEY]
        else:
            self.key = '{00000000-0000-0000-0000-000000000000}'
        if ACTION in data:
            self.action = data[ACTION]
        self.invalid = FLAGS in data and INVALID in data[FLAGS]
        # if not self.invalid:
        if DATA in data and data[DATA] is not None and VALUE in data[DATA]:
            self.value = data[DATA][VALUE]

    def __str__(self):
        if self.action == GET:
            return str.format('class: {}, id: {}, GET', self.cl, self.id)
        elif ERROR in self.data:
            return str.format('class: {}, id: {}, error: {}', self.cl, self.id, self.data['error'])
        else:
            return super(VariableJocket, self).__str__()

    def pack(self):
        data = self.data
        if CLASS[:-1] in data[ADDR]:
            data[ADDR][CLASS] = data[ADDR].pop(CLASS[:-1])
        if self.invalid:
            if FLAGS not in data:
                data[FLAGS] = []
            if INVALID not in data[FLAGS]:
                data[FLAGS].append(INVALID)
        data[KEY] = self.key
        data[ACTION] = self.action
        return json.dumps(data)

    @staticmethod
    def create_data(id, cl, action=GET, val=None, key=None, invalid=False):
        if val is None or val == '':
            print('AHTUNG on create_data')
        data = dict()
        data[ADDR] = dict()
        data[ADDR][ID] = id
        data[ADDR][CLASS] = cl
        if key:
            data[KEY] = str(key)
        else:
            data[KEY] = '{00000000-0000-0000-0000-000000000000}'
        data[ACTION] = action
        data[TIMESTAMP] = str(datetime.now()).replace(' ', 'T')
        data[DATA] = dict()
        data[DATA][VALUE] = val
        if invalid:
            if FLAGS not in data:
                data[FLAGS] = []
            if INVALID not in data[FLAGS]:
                data[FLAGS].append(INVALID)
        return VariableJocket(data)


class VariableTRS3(Variable):
    isList = False
    hasPassword = False

    def __init__(self, r=None, id=0, cl=0, val=None, invalid=False):
        super().__init__(id, cl, val)
        if r:
            data = r.read_data(5)
            varId = VarId(var_id=struct.unpack('<i', data[:4])[0])
            self.id = varId.dev_id
            self.cl = varId.var_ind
            self.varId = varId
            attr = data[4]
            self.invalid = attr & 0x3 == 0x0
            if attr & 0x1 == 0x1:
                vl = Value(r)
                self.value = vl.data
            if attr & 0x2 == 0x2:
                self.timeStamp = DateTimeTRS3(r)
            if attr & 0x4 == 0x4:
                self.isList = True
            if attr & 0x8 == 0x8:
                self.hasPassword = True
        else:
            self.timeStamp = DateTimeTRS3.get()
            self.invalid = invalid

    def pack(self):
        res = bytearray(struct.pack('<i', VarId(dev_id=self.id, var_ind=self.cl).var_id))
        attr = 0x0
        if not self.invalid:
            attr += 0x1
            v = self.value
            if isinstance(v, bool):
                res += b'\x02'
                res += struct.pack('<?', v)
            elif isinstance(v, int):
                res += b'\x01'
                res += struct.pack('<i', v)
            elif isinstance(v, str):
                res += b'\x04'
                res += struct.pack('<i', len(v))
                res += v.encode()
            elif isinstance(self.value, DateTime):
                res += b'\x08'
                res += self.value.pack()
            else:
                attr -= 0x1
            if self.timeStamp:
                attr += 0x2
                res += self.timeStamp.pack()
        res.insert(4, attr)
        return res


class VarId:
    def __init__(self, var_id=0, dev_id=0, var_ind=0):
        self.var_id = var_id
        self.var_ind = var_ind
        self.dev_id = dev_id
        if var_id:
            self.dev_id, self.var_ind = self.parse(var_id)
        else:
            self.var_id = self.build_var_id(dev_id, var_ind)

    def __str__(self):
        return str(dict(var_id=self.var_id, var_ind=self.var_ind, dev_id=self.dev_id))

    def __repr__(self):
        return str(self)

    def build_var_id(self, dev_id, var_idx):
        return (dev_id << 8) + var_idx

    def parse(self, var_id):
        return var_id >> 8, var_id & 0xFF


class DateTime:
    year = 0
    mon = 0
    day = 0
    hour = 0
    min = 0
    sec = 0
    ms = 0
    date_str = None

    def __str__(self):
        return self.date_str

    def __repr__(self):
        return str(self)

    def pack(self):
        pass


class DateTimeJocket(DateTime):
    def __init__(self, date_str):
        super().__init__()
        self.date_str = date_str
        res = date_str.split('T')
        date = res[0].split('-')
        self.year = int(date[0])
        self.mon = int(date[1])
        self.day = int(date[2])
        time = res[1].split(':')
        self.hour = int(time[0])
        self.min = int(time[1])
        sec = time[2].split('.')
        self.sec = int(sec[0])
        self.ms = int(sec[1]) if len(sec) > 1 else 0


class DateTimeTRS3(DateTime):
    @staticmethod
    def get():
        ts = datetime.now()
        return DateTimeTRS3(
            VariableReader(
                bytes([1]) + int(ts.year).to_bytes(2, 'little') + bytes([ts.month, ts.day, ts.hour, ts.minute, ts.second]) + int(ts.microsecond/1000).to_bytes(2, 'little')
            )
        )

    def __init__(self, r):
        super().__init__()
        data = r.read_data(10)
        self.year = struct.unpack('<h', data[1:3])[0]
        self.mon = data[3]
        self.day = data[4]
        self.hour = data[5]
        self.min = data[6]
        self.sec = data[7]
        self.ms = struct.unpack('<h', data[8:])[0]
        res = str.format('{}-{}-{}T{}:{}:{}.{}', self.year, self.mon, self.day, self.hour, self.min, self.sec, self.ms)
        res = res.split('T')
        for re in res:
            index = res.index(re)
            sep = ':' if ':' in re else '-'
            re = re.split(sep)
            for r in re:
                if '.' in r:
                    index2 = re.index(r)
                    r = r.split('.')
                    for q in r:
                        if r.index(q) == 0 and len(q) < 2:
                            r[r.index(q)] = '0' + q
                        elif r.index(q) == 1 and len(q) < 3:
                            r[r.index(q)] = '0' * (3 - len(q)) + q
                    re[index2] = '.'.join(r)
                elif len(r) < 2:
                    re[re.index(r)] = '0' + str(r)
            res[index] = sep.join(re)
        self.date_str = 'T'.join(res)

    def pack(self):
        res = bytearray(b'\x7f')
        res += struct.pack('<h', self.year)
        res += self.mon.to_bytes(1, 'little')
        res += self.day.to_bytes(1, 'little')
        res += self.hour.to_bytes(1, 'little')
        res += self.min.to_bytes(1, 'little')
        res += self.sec.to_bytes(1, 'little')
        res += struct.pack('<h', self.ms)
        return res


class Value:
    type = 0
    data = None

    def __init__(self, r):
        data = r.read_data(1)
        self.type = int.from_bytes(data, 'little')
        if self.type > 0:
            frm, size, cl = self.frm_size()
            if cl:
                size = struct.unpack('<I', r.read_data(size))[0]
                if cl == str:
                    _data = r.read_data(size)
                    self.data = _data.decode()
                else:
                    self.data = cl(r.read_data(size))
            else:
                self.data = struct.unpack(frm, r.read_data(size))[0]

    def __str__(self):
        return str.format('[{}]: {}', self.type, str(self.data))

    def __repr__(self):
        return str(self)

    def frm_size(self):
        if self.type == 1:
            return '<i', 4, None
        elif self.type == 2:
            return '<?', 1, None
        elif self.type == 3:
            return '<d', 8, None
        elif self.type == 4:
            return '<{}s', 4, str
        elif self.type == 7:
            return '', 4, DateTime


class VariableReader:
    def __init__(self, data):
        self.last_index = 0
        self.data = data

    def read_data(self, size):
        index = self.last_index
        self.last_index += size
        return self.data[index:(index + size)]
