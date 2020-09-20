import lom
from spread_core.errors.project_errors import TopicError
from spread_core.mqtt.bus import *
from spread_core.mqtt.data import *
from spread_core.protocols.dali.device import general as device, light_sensor_ext as device_light, \
    presence_sensor_ext as device_presence
from spread_core.protocols.dali.gear import led as led, general as gear
from spread_core.protocols.mercury import commands as mercury

TPLUS = '+'
TSHARP = '#'
TVALUE = '{}'

# DATA
# {ProjectID}/Jocket/State/Hardware/{ServerType}/{ServerID}/{ManagerType}/{ManagerID}/{ProviderType}/{ProviderID}/{FunctionUnitType}
# {ProjectID}/Jocket/Command/SessionID/Hardware/{ServerType}/{ServerID}/{ManagerType}/{ManagerID}/{ProviderType}/{ProviderID}/{FunctionUnitType}
# {ProjectID}/Jocket/Reply/{Session_ID}/{Key}
JOCKET = 'Jocket'
TROS3 = 'Tros3'
PROJECT = 'Project'
FILE = 'File'
ENTITY = 'Entity'
STATE = 'State'
COMMAND = 'Command'
REPLY = 'Reply'
HARDWARE = 'Hardware'
EQUIPMENT = 'Equipment'
VARIANTS = 'variants'
DEF_VAL = 'def_value'
MASK = 'mask'

# BUS
# 'Bus/Dump/Rapida/{DeviceID}/Can/{CanID}'
# 'Bus/Send/Rapida/{DeviceID}/Dali/{ModuleID}/{ChannelID}'
S_TYPE = 'AppServer'
BUS = 'Bus'
DUMP = 'Dump'
SEND = 'Send'
RAPIDA = 'Rapida'
CAN = 'Can'
DALI = 'Dali'
MODULE = 'Module'
MERCURY = 'Mercury'


DOUBLE_SEND_FLAG = 'D'
THREE_BYTES_FLAG = 'F'
FORCE_ANSWER_FLAG = 'W'
DALI_ERROR_FLAG = 'E'

LOGARITHMIC_CURVE = "Logarithmic"
LINEAR_CURVE = "Linear"

_DEF_GROUPS = [{"index": 0, "value": False}, {"index": 15, "value": False}, {"index": 1, "value": False}, {"index": 2, "value": False}, {"index": 4, "value": False}, {"index": 5, "value": False}, {"index": 6, "value": False}, {"index": 7, "value": False}, {"index": 8, "value": False}, {"index": 9, "value": False}, {"index": 10, "value": False}, {"index": 11, "value": False}, {"index": 12, "value": False}, {"index": 13, "value": False}, {"index": 14, "value": False}, {"index": 3, "value": False}]
_DEF_SCENES = [{"index": 0, "value": 255}, {"index": 1, "value": 255}, {"index": 2, "value": 255}, {"index": 3, "value": 255}, {"index": 4, "value": 255}, {"index": 5, "value": 255}, {"index": 6, "value": 255}, {"index": 7, "value": 255}, {"index": 8, "value": 255}, {"index": 9, "value": 255}, {"index": 10, "value": 255}, {"index": 11, "value": 255}, {"index": 12, "value": 255}, {"index": 13, "value": 255}, {"index": 14, "value": 255}, {"index": 15, "value": 255}]


def of(topic):
    arr = topic.split('/')
    try:
        if arr[0] == JOCKET:
            type = arr[1]
            if type == REPLY:
                return TopicReply(arr[2], *arr[3:])
            elif type == STATE:
                return TopicState(arr[2], EntityAddress.of(*arr[3:]))
            elif type == COMMAND:
                return TopicCommand(arr[2], arr[3], EntityAddress.of(*arr[4:]))
        elif arr[0] == TROS3:
            if arr[1] == COMMAND:
                return TopicCommandTros3(arr[2], EngineryAddress(*arr[3:]))
            elif arr[1] == STATE:
                return TopicStateTros3(arr[2], EngineryAddress(*arr[3:]))
        elif arr[0] == BUS:
            if arr[2] == MERCURY:
                if 'error' in arr:
                    return TopicTcpError(arr[1], arr[3])
                else:
                    return TopicTcp(arr[1], arr[3])
            type = arr[4]
            if type == CAN:
                return TopicCan(arr[1], int(arr[3]), int(arr[5]), int(arr[6]))
            elif type == DALI:
                return TopicDali(arr[1], int(arr[3]), int(arr[5]), arr[6])
        elif arr[0] == PROJECT:
            return TopicProject(*arr[2:])
    except AddressError as ex:
        raise TopicError("Couldn't parse address of " + topic)
    else:
        raise TopicError(f'Unsupported topic "{topic}"')


unpack_formats = dict(int='<I', float='<d', bool='<?')


def get_unpack_frmt(funit, data_len):
    tp = None
    if 'set' in funit:
        tp = funit['set']
    elif 'state' in funit:
        tp = funit['state']
    elif 'get' in funit:
        tp = funit['get']
    elif 'value' in funit:
        tp = funit['value']

    if tp == dict or tp == list:
        tp = None

    if tp is not None and tp.__name__ in unpack_formats:
        return unpack_formats[tp.__name__]

    return str.format('<{}s', data_len)


classifier = dict(
    # Variable::Lighting::Self
    Lighting=dict(
        Lighting=dict(On=dict(id=1010001),
                      IsMatchScene1=dict(id=1010003),
                      IsMatchScene2=dict(id=1010004),
                      PowerLevel=dict(id=1010005),
                      IsLightSensorsOn=dict(id=1010006),
                      IsPresenceSensorsOn=dict(id=1010008)),

        # Variable::Lighting::SwitchingLight
        SwitchingLight=dict(On=dict(id=1010101),
                            PowerLevel=dict(id=1010102)),

        # Variable::Lighting::DimmingLight
        DimmingLight=dict(On=dict(id=1010201),
                          BrightnessLevel=dict(id=1010202),
                          PowerLevel=dict(id=1010203)),

        # Variable::Lighting::RgbLight
        RgbLight=dict(On=dict(id=1010301),
                      RgbBrightnessLevel=dict(id=1010302)),

        # Variable::Lighting::DynamicLight
        DynamicLight=dict(On=dict(id=1010401),
                          ScenarioNumber=dict(id=1010402)),

        # Variable::Lighting::LightSensor
        LightSensor=dict(On=dict(id=1010501)),

        # Variable::Lighting::PresenceSensor
        PresenceSensor=dict(On=dict(id=1010601)),

        # Equipment::Lighting::LightingArea
        LightingArea=dict(On=dict(id=1010701, get=dict(value=None), set=dict(value=None), state=bool),
                          Discovery=dict(id=1010702, get=dict(value=None), set=dict(func='SetDiscovery'), state=bool),
                          TuningType=dict(id=1010721, get=dict(value=None), set=dict(value=None), variants=['Combo', 'Presence', 'Luminosity'], state=str, def_value='Combo'),
                          OccupancyLevel=dict(id=1010722, get=dict(value=None), set=dict(value=None), state=int),
                          VacancyLevel=dict(id=1010723, get=dict(value=None), set=dict(value=None), state=int),
                          TargetLuminosity=dict(id=1010724, get=dict(value=None), set=dict(value=None), state=int),
                          Hysteresis=dict(id=1010725, get=dict(value=None), set=dict(value=None), state=int),
                          TuningSpeed=dict(id=1010726, get=dict(value=None), set=dict(value=None), state=int),
                          OccupancyAction=dict(id=1010727, get=dict(value=None), set=dict(value=None), variants=['LastLevel', 'MaxLevel', 'Level', 'Scene'], state=str, def_value='LastLevel'),
                          OccupancyScene=dict(id=1010728, get=dict(value=None), set=dict(value=None), state=int),
                          VacancyAction=dict(id=1010729, get=dict(value=None), set=dict(value=None), variants=['MinLevel', 'Level', 'Scene', 'Off'], state=str, def_value='MinLevel'),
                          VacancyScene=dict(id=1010730, get=dict(value=None), set=dict(value=None), state=int),
                          Presence=dict(id=1010771, get=dict(value=None), state=bool),
                          Luminosity=dict(id=1010772, get=dict(value=None), state=int))),

    # Hardware::Lite::RapidaDali::RapidaDaliDimmer
    RapidaDali=dict(
        Bus=dict(id=31090001, get=dict(func='GetScan'), run=dict(func='scan')),
        Ready=dict(id=31090002, state=bool),
        RapidaDaliDimmer=dict(
            BrightnessLevel=dict(id=31090102, set=dict(func='SetBrightnessLevel'), get=(dict(func='GetBrightnessLevel')), state=int),
            Address=dict(id=31090110, set=dict(func='SetShortAddress')), # null - удаление адреса)
            Types=dict(id=31090112, get=dict(func='GetTypes'), state=list),
            Discovery=dict(id=31090113, set=dict(func='SetDiscovery'), get=dict(func='GetDiscovery'), state=bool),
            CurrentLevelRaw=dict(id=31090114, set=dict(cmd=gear.DAPC), get=dict(cmd=gear.QueryActualLevel), state=int),
            CurrentLevel=dict(id=31090115),
            PhysicalMinLevelRaw=dict(id=31090116, get=dict(cmd=gear.QueryPhysicalMinimum), state=int),
            PhysicalMinLevel=dict(id=31090117),
            MinLevelRaw=dict(id=31090118, set=dict(cmd=gear.SetMinLevel), get=dict(cmd=gear.QueryMinLevel), state=int, def_value=1),
            MinLevel=dict(id=31090119),
            MaxLevelRaw=dict(id=31090120, set=dict(cmd=gear.SetMaxLevel), get=dict(cmd=gear.QueryMaxLevel), state=int),
            MaxLevel=dict(id=31090121),
            PowerOnLevelRaw=dict(id=31090122, set=dict(cmd=gear.SetPowerOnLevel), get=dict(cmd=gear.QueryPowerOnLevel), state=int),
            PowerOnLevel=dict(id=31090123),
            SystemFailureLevelRaw=dict(id=31090124, set=dict(cmd=gear.SetSystemFailureLevel), get=dict(cmd=gear.QuerySystemFailureLevel), state=int, def_value=255),
            SystemFailureLevel=dict(id=31090125),
            SceneLevelsRaw=dict(id=31090126, set=dict(func='SetSceneLevelsRaw'), get=dict(func='GetSceneLevelsRaw'), state=list, def_value=_DEF_SCENES),
            SceneLevels=dict(id=31090127),
            Groups=dict(id=31090128, set=dict(func='SetGroups'), get=dict(func='GetGroups'), state=list, def_value=_DEF_GROUPS),
            FadeTime=dict(id=31090129, set=dict(cmd=gear.SetFadeTime), get=dict(cmd=gear.QueryFadeTimeFadeRate), state=str, variants=["ft0", "ft7", "ft10", "ft14", "ft20", "ft28", "ft40", "ft57", "ft80", "ft113", "ft160", "ft226", "ft320", "ft453", "ft640", "ft905"], def_value='ft0'),
            FadeRate=dict(id=31090130, set=dict(cmd=gear.SetFadeRate), get=dict(cmd=gear.QueryFadeTimeFadeRate), state=str, variants=["fr3580", "fr2530", "fr1790", "fr1270", "fr894", "fr633", "fr447", "fr316", "fr224", "fr158", "fr112", "fr79", "fr56", "fr40", "fr28"], def_value='fr3580'),
            DimmingCurve=dict(id=31090131, set=dict(cmd=led.SelectDimmingCurve), get=dict(cmd=led.QueryDimmingCurve), state=str, variants=[LOGARITHMIC_CURVE, LINEAR_CURVE], def_value=LOGARITHMIC_CURVE),
            GroupLevel=dict(id=31090132, set=dict(func='SetGroupLevel'), get=dict(value=True), state=int),
            Gtin=dict(id=31090141, get=dict(func='Gtin'), state=str),
            Serial=dict(id=31090142, get=dict(func='Serial'), state=str),
            GtinOem=dict(id=31090143, get=dict(func='GtinOem'), state=str),
            SerialOem=dict(id=31090144, get=dict(func='SerialOem'), state=str),
            FirmwareVersion=dict(id=31090145, get=dict(func='FirmwareVersion'), state=str),
            HardwareVersion=dict(id=31090146, get=dict(func='HardwareVersion'), state=str),
            Binding=dict(id=31090181, set=dict(func='SetBinding'), get=dict(func='GetBinding'), reset=dict(func='ResetBinding'), def_value='Device', state=str),
            BindingDevice=dict(id=31090182, set=dict(func='SetBindingDevice'), get=dict(func='GetBindingDevice'), reset=dict(func='ResetBindingDevice'), state=int),
            BindingGroup=dict(id=31090183, set=dict(func='SetBindingGroup'), get=dict(func='GetBindingGroup'), reset=dict(func='ResetBindingGroup'), state=int),
            Tuning=dict(id=31090191, get=dict(value=None), set=dict(func='SetTuning'), state=float),
            Scene=dict(id=31090192, set=dict(cmd=gear.GoToScene), state=int),
            LastLevel=dict(id=31090193, get=dict(value=True), set=dict(value=True), state=int),
            RecallMaxLevel=dict(id=31090194, set=dict(cmd=gear.RecallMaxLevel)),
            RecallMinLevel=dict(id=31090195, set=dict(cmd=gear.RecallMinLevel))
        ),
        RapidaDaliTunableWhite=dict(
            BrightnessLevel=dict(id=31090502, set=dict(func='SetBrightnessLevel'), get=(dict(func='GetBrightnessLevel')), state=int),
            Address=dict(id=31090510, set=dict(func='SetShortAddress')), # null - удаление адреса)
            Types=dict(id=31090512, get=dict(func='GetTypes'), state=list),
            Discovery=dict(id=31090513, set=dict(func='SetDiscovery'), get=dict(func='GetDiscovery'), state=bool),
            CurrentLevelRaw=dict(id=31090514, set=dict(cmd=gear.DAPC), get=dict(cmd=gear.QueryActualLevel), state=int),
            CurrentLevel=dict(id=31090515),
            PhysicalMinLevelRaw=dict(id=31090516, get=dict(cmd=gear.QueryPhysicalMinimum), state=int),
            PhysicalMinLevel=dict(id=31090517),
            MinLevelRaw=dict(id=31090518, set=dict(cmd=gear.SetMinLevel), get=dict(cmd=gear.QueryMinLevel), state=int, def_value=1),
            MinLevel=dict(id=31090519),
            MaxLevelRaw=dict(id=31090520, set=dict(cmd=gear.SetMaxLevel), get=dict(cmd=gear.QueryMaxLevel), state=int),
            MaxLevel=dict(id=31090521),
            PowerOnLevelRaw=dict(id=31090522, set=dict(cmd=gear.SetPowerOnLevel), get=dict(cmd=gear.QueryPowerOnLevel), state=int),
            PowerOnLevel=dict(id=31090523),
            SystemFailureLevelRaw=dict(id=31090524, set=dict(cmd=gear.SetSystemFailureLevel), get=dict(cmd=gear.QuerySystemFailureLevel), state=int, def_value=255),
            SystemFailureLevel=dict(id=31090525),
            SceneLevelsRaw=dict(id=31090526, set=dict(func='SetSceneLevelsRaw'), get=dict(func='GetSceneLevelsRaw'), state=list, def_value=_DEF_SCENES),
            SceneLevels=dict(id=31090527),
            Groups=dict(id=31090528, set=dict(func='SetGroups'), get=dict(func='GetGroups'), state=list, def_value=_DEF_GROUPS),
            FadeTime=dict(id=31090529, set=dict(cmd=gear.SetFadeTime), get=dict(cmd=gear.QueryFadeTimeFadeRate), state=str, variants=["ft0", "ft7", "ft10", "ft14", "ft20", "ft28", "ft40", "ft57", "ft80", "ft113", "ft160", "ft226", "ft320", "ft453", "ft640", "ft905"], def_value='ft0'),
            FadeRate=dict(id=31090530, set=dict(cmd=gear.SetFadeRate), get=dict(cmd=gear.QueryFadeTimeFadeRate), state=str, variants=["fr3580", "fr2530", "fr1790", "fr1270", "fr894", "fr633", "fr447", "fr316", "fr224", "fr158", "fr112", "fr79", "fr56", "fr40", "fr28"], def_value='fr3580'),
            DimmingCurve=dict(id=31090531, set=dict(cmd=led.SelectDimmingCurve), get=dict(cmd=led.QueryDimmingCurve), state=str, variants=[LOGARITHMIC_CURVE, LINEAR_CURVE], def_value=LOGARITHMIC_CURVE),
            GroupLevel=dict(id=31090532, set=dict(func='SetGroupLevel'), get=dict(value=True), state=int),
            Gtin=dict(id=31090541, get=dict(func='Gtin'), state=str),
            Serial=dict(id=31090542, get=dict(func='Serial'), state=str),
            GtinOem=dict(id=31090543, get=dict(func='GtinOem'), state=str),
            SerialOem=dict(id=31090544, get=dict(func='SerialOem'), state=str),
            FirmwareVersion=dict(id=31090545, get=dict(func='FirmwareVersion'), state=str),
            HardwareVersion=dict(id=31090546, get=dict(func='HardwareVersion'), state=str),
            Binding=dict(id=31090581, set=dict(func='SetBinding'), get=dict(func='GetBinding'), reset=dict(func='ResetBinding'), def_value='Device', state=str),
            BindingDevice=dict(id=31090582, set=dict(func='SetBindingDevice'), get=dict(func='GetBindingDevice'), reset=dict(func='ResetBindingDevice'), state=int),
            BindingGroup=dict(id=31090583, set=dict(func='SetBindingGroup'), get=dict(func='GetBindingGroup'), reset=dict(func='ResetBindingGroup'), state=int),
            Tuning=dict(id=31090591, get=dict(value=None), set=dict(func='SetTuning'), state=float),
            Scene=dict(id=31090592, set=dict(cmd=gear.GoToScene), state=int),
            LastLevel=dict(id=31090593, get=dict(value=True), set=dict(value=True), state=int),
            RecallMaxLevel=dict(id=31090594, set=dict(cmd=gear.RecallMaxLevel)),
            RecallMinLevel=dict(id=31090595, set=dict(cmd=gear.RecallMinLevel)),
            Temperature=dict(id=31090596, set=dict(func='SetTemperature'), get=dict(func='GetTemperature'), state=int),
            Coolest=dict(id=31090597, set=dict(func='SetCoolest'), get=dict(func='GetCoolest'), state=int),
            Warmest=dict(id=31090596, set=dict(func='SetWarmest'), get=dict(func='GetWarmest'), state=int)
        ),

        # Hardware::Lite::RapidaDali::RapidaDaliRelay
        RapidaDaliRelay=dict(
            BrightnessLevel=dict(id=31090202, set=dict(func='SetBrightnessLevel'), get=(dict(func='GetBrightnessLevel')), state=int),
            Address=dict(id=31090210, set=dict(func='SetShortAddress')), # null - удаление адреса)
            Types=dict(id=31090212, get=dict(func='GetTypes'), state=list, value=int),
            Discovery=dict(id=31090213, set=dict(func='SetDiscovery'), get=dict(func='GetDiscovery'), state=bool),
            CurrentLevelRaw=dict(id=31090214, set=dict(cmd=gear.DAPC), get=dict(cmd=gear.QueryActualLevel), state=int),
            CurrentLevel=dict(id=31090215),
            PhysicalMinLevelRaw=dict(id=31090216, get=dict(cmd=gear.QueryPhysicalMinimum), state=int),
            PhysicalMinLevel=dict(id=31090217),
            MinLevelRaw=dict(id=31090218, set=dict(cmd=gear.SetMinLevel), get=dict(cmd=gear.QueryMinLevel), state=int, def_value=1),
            MinLevel=dict(id=31090219),
            MaxLevelRaw=dict(id=31090220, set=dict(cmd=gear.SetMaxLevel), get=dict(cmd=gear.QueryMaxLevel), state=int),
            MaxLevel=dict(id=31090221),
            PowerOnLevelRaw=dict(id=31090222, set=dict(cmd=gear.SetPowerOnLevel), get=dict(cmd=gear.QueryPowerOnLevel), state=int),
            PowerOnLevel=dict(id=31090223),
            SystemFailureLevelRaw=dict(id=31090224, set=dict(cmd=gear.SetSystemFailureLevel), get=dict(cmd=gear.QuerySystemFailureLevel), state=int),
            SystemFailureLevel=dict(id=31090225),
            SceneLevelsRaw=dict(id=31090226, set=dict(func='SetSceneLevelsRaw'), get=dict(func='GetSceneLevelsRaw'), state=list, def_value=_DEF_SCENES),
            SceneLevels=dict(id=31090227),
            Groups=dict(id=31090228, set=dict(func='SetGroups'), get=dict(func='GetGroups'), state=list, def_value=_DEF_GROUPS),
            FadeTime=dict(id=31090229, set=dict(cmd=gear.SetFadeTime), get=dict(cmd=gear.QueryFadeTimeFadeRate), state=str, variants=["ft0", "ft7", "ft10", "ft14", "ft20", "ft28", "ft40", "ft57", "ft80", "ft113", "ft160", "ft226", "ft320", "ft453", "ft640", "ft905"], def_value='ft0'),
            FadeRate=dict(id=31090230, set=dict(cmd=gear.SetFadeRate), get=dict(cmd=gear.QueryFadeTimeFadeRate), state=str, variants=["fr3580", "fr2530", "fr1790", "fr1270", "fr894", "fr633", "fr447", "fr316", "fr224", "fr158", "fr112", "fr79", "fr56", "fr40", "fr28"], def_value='fr3580'),
            DimmingCurve=dict(id=31090231, set=dict(cmd=led.SelectDimmingCurve), get=dict(cmd=led.QueryDimmingCurve), state=str, variants=[LOGARITHMIC_CURVE, LINEAR_CURVE], def_value=LOGARITHMIC_CURVE),
            GroupLevel=dict(id=31090232, set=dict(func='SetGroupLevel'), get=dict(value=True), state=int),
            Gtin=dict(id=31090241, get=dict(func='Gtin'), state=str),
            Serial=dict(id=31090242, get=dict(func='Serial'), state=str),
            GtinOem=dict(id=31090243, get=dict(func='GtinOem'), state=str),
            SerialOem=dict(id=31090244, get=dict(func='SerialOem'), state=str),
            FirmwareVersion=dict(id=31090245, get=dict(func='FirmwareVersion'), state=str),
            HardwareVersion=dict(id=31090246, get=dict(func='HardwareVersion'), state=str),
            Binding=dict(id=31090281, set=dict(func='SetBinding'), get=dict(func='GetBinding'), reset=dict(func='ResetBinding'), def_value='Device', state=str),
            BindingDevice=dict(id=31090282, set=dict(func='SetBindingDevice'), get=dict(func='GetBindingDevice'), reset=dict(func='ResetBindingDevice'), state=int),
            BindingGroup=dict(id=31090283, set=dict(func='SetBindingGroup'), get=dict(func='GetBindingGroup'), reset=dict(func='ResetBindingGroup'), state=int),
            Scene=dict(id=31090292, set=dict(cmd=gear.GoToScene), state=int),
            LastLevel=dict(id=31090293, get=dict(value=True), set=dict(value=True), state=int),
            RecallMaxLevel=dict(id=31090294, set=dict(cmd=gear.RecallMaxLevel)),
            RecallMinLevel=dict(id=31090295, set=dict(cmd=gear.RecallMinLevel))
        ),

        # 31-09-03-00 	Hardware::Lite::RapidaDali::RapidaDaliCombiLight
        RapidaDaliCombiLight=dict(On=dict(id=31090301),
                                  Trigger=dict(id=31090302),
                                  Address=dict(id=31090310, set=int), # null - удаление адреса
                                  Types=dict(id=31090312, state=list, value=int),
                                  Discovery=dict(id=31090313, set=bool, state=bool),
                                  TargetGroup=dict(id=31090314, set=int, state=int),
                                  Mode=dict(id=31090315, set=str, state=str),
                                  TuningSpeed=dict(id=31090316, set=str, state=str),
                                  TargetLuminosity=dict(id=31090317, set=int, state=int),
                                  CurrentLuminosity=dict(id=31090318, state=int),
                                  Gtin=dict(id=31090341, state=str),
                                  Serial=dict(id=31090342, state=str),
                                  GtinOem=dict(id=31090343, state=str),
                                  SerialOem=dict(id=31090344, state=str),
                                  FirmwareVersion=dict(id=31090345, state=str),
                                  HardwareVersion=dict(id=31090346, state=str),
                                  Binding=dict(id=31090381, set=str, state=str), # {"Device", "Group", "Broadcast"}
                                  BindingDevice=dict(id=31090382, set=int, state=int),
                                  BindingGroup=dict(id=31090383, set=int, state=int)),

        # Hardware::Lite::RapidaDali::RapidaDaliCombiPresence
        RapidaDaliCombiPresence=dict(On=dict(id=31090401),
                                     Trigger=dict(id=31090402),
                                     Address=dict(id=31090410, set=int), # null - удаление адреса
                                     Types=dict(id=31090412, state=list, value=int),
                                     Discovery=dict(id=31090413, set=bool, state=bool),
                                     TargetGroup=dict(id=31090414, set=int, state=int),
                                     Delay=dict(id=31090415, set=str, state=str),
                                     Sensitivity=dict(id=31090416, set=int, state=int),
                                     TargetLevelRaw=dict(id=31090417, set=int, state=int),
                                     TargetLevel=dict(id=31090418, set=int, state=int),
                                     TestTarget=dict(id=31090419),
                                     Gtin=dict(id=31090441, state=str),
                                     Serial=dict(id=31090442, state=str),
                                     GtinOem=dict(id=31090443, state=str),
                                     SerialOem=dict(id=31090444, state=str),
                                     FirmwareVersion=dict(id=31090445, state=str),
                                     HardwareVersion=dict(id=31090446, state=str),
                                     Binding=dict(id=31090481, set=str, state=str), # {"Device", "Group", "Broadcast"}
                                     BindingDevice=dict(id=31090482, set=int, state=int),
                                     BindingGroup=dict(id=31090483, set=int, state=int)),

        # 31090600 	31-09-06-00 	Hardware::Lite::RapidaDali::RapidaDaliLightSensor
        RapidaDaliLightSensor=dict(
            On=dict(id=31090601, set=dict(func='SetEnable'), get=dict(cmd=device.QueryInstanceEnabled), state=bool),
            Discovery=dict(id=31090602, set=dict(func='SetDiscovery'), get=dict(func='GetDiscovery'), state=bool),
            Binding=dict(id=31090611, set=dict(func='SetBinding'), get=dict(func='GetBinding'), reset=dict(func='ResetBinding'), def_value='Device', state=str),
            BindingDevice=dict(id=31090612, set=dict(func='SetBindingDevice'), get=dict(func='GetBindingDevice'), reset=dict(func='ResetBindingDevice'), state=int),
            BindingGroup=dict(id=31090613, set=dict(func='SetBindingGroup'), get=dict(func='GetBindingGroup'), reset=dict(func='ResetBindingGroup'), state=int),
            Address=dict(id=31090621, set=dict(func='SetShortAddress')), # null - удаление адреса
            OperationMode=dict(id=31090622, set=dict(cmd=device.SetOperatingMode), get=dict(cmd=device.QueryOperatingMode), state=int),
            Groups=dict(id=31090623, set=dict(func='SetGroups'), get=dict(func='GetGroups'), state=list),
            Group0=dict(id=31090624, get=dict(cmd=device.QueryPrimaryInstanceGroup), set=dict(cmd=device.SetPrimaryInstanceGroup), state=int),
            Group1=dict(id=31090625, get=dict(cmd=device.QueryInstanceGroup1), set=dict(cmd=device.SetInstanceGroup1), state=int),
            Group2=dict(id=31090626, get=dict(cmd=device.QueryInstanceGroup2), set=dict(cmd=device.SetInstanceGroup2), state=int),
            EventScheme=dict(id=31090627, set=dict(cmd=device.SetEventScheme), get=dict(cmd=device.QueryEventScheme), state=str, variants=["esIndexAndType", "esAddressAndType", "esAddressAndIndex", "esLowestGroupAndType", "esPrimaryGroupAndType"], def_value='esIndexAndType'),
            EventPriority=dict(id=31090628, get=dict(cmd=device.QueryEventPriority), set=dict(cmd=device.SetEventPriority), state=int),
            EventFilter=dict(id=31090629, set=dict(cmd=device.SetEventFilter), get=dict(cmd=device.QueryEventFilterL), state=list, mask=["illuminanceLevelEvents"]),
            DeadTime=dict(id=31090630, get=dict(cmd=device_light.QueryDeadTime), set=dict(cmd=device_light.SetDeadTime), state=int),
            ReportTime=dict(id=31090631, get=dict(cmd=device_light.QueryReportTime), set=dict(cmd=device_light.SetReportTime), state=int),
            Hysteresis=dict(id=31090632, get=dict(cmd=device_light.QueryHysteresis), set=dict(cmd=device_light.SetHysteresis), state=int),
            HysteresisMin=dict(id=31090633, get=dict(cmd=device_light.QueryHysteresisMin), set=dict(cmd=device_light.SetHysteresisMin), state=int),
            Gtin=dict(id=31090641, get=dict(func='Gtin'), state=str),
            Serial=dict(id=31090642, get=dict(func='Serial'), state=str),
            GtinOem=dict(id=31090643, get=dict(func='GtinOem'), state=str),
            SerialOem=dict(id=31090644, get=dict(func='SerialOem'), state=str),
            FirmwareVersion=dict(id=31090645, get=dict(func='FirmwareVersion'), state=str),
            HardwareVersion=dict(id=31090646, get=dict(func='HardwareVersion'), state=str),
            InstancesNumber=dict(id=31090647, get=dict(cmd=device.QueryNumberOfInstances), state=int),
            InstanceIndex=dict(id=31090648, get=dict(func='GetInstanceIndex'), state=int),
            InstanceType=dict(id=31090649, get=dict(value=None), state=int),
            FeatureTypes=dict(id=31090650, get=dict(func='GetFeatureTypes'), state=int),
            Resolution=dict(id=31090651, get=dict(cmd=device.QueryResolution), state=int),
            Event=dict(id=31090661, state=int), # (Last Event Data, 10bit)
            CurrentLuminosity=dict(id=31090662, get=dict(func='GetCurrentLuminosity'), state=int)
        ),


        # 31090700 	31-09-07-00 	Hardware::Lite::RapidaDali::RapidaDaliPresenceSensor::Self
        RapidaDaliPresenceSensor=dict(
            On=dict(id=31090701, set=dict(func='SetEnable'), get=dict(cmd=device.QueryInstanceEnabled), state=bool),
            Discovery=dict(id=31090702, set=dict(func='SetDiscovery'), get=dict(func='GetDiscovery'), state=bool),
            Binding=dict(id=31090711, set=dict(func='SetBinding'), get=dict(func='GetBinding'), reset=dict(func='ResetBinding'), def_value='Device', state=str),
            BindingDevice=dict(id=31090712, set=dict(func='SetBindingDevice'), get=dict(func='GetBindingDevice'), reset=dict(func='ResetBindingDevice'), state=int),
            BindingGroup=dict(id=31090713, set=dict(func='SetBindingGroup'), get=dict(func='GetBindingGroup'), reset=dict(func='ResetBindingGroup'), state=int),
            Address=dict(id=31090721, set=dict(func='SetShortAddress')), # null - удаление адреса
            OperationMode=dict(id=31090722, set=dict(cmd=device.SetOperatingMode), get=dict(cmd=device.QueryOperatingMode), state=int),
            Groups=dict(id=31090723, set=dict(func='SetGroups'), get=dict(func='GetGroups'), state=list),
            Group0=dict(id=31090724, get=dict(cmd=device.QueryPrimaryInstanceGroup), set=dict(cmd=device.SetPrimaryInstanceGroup), state=int),
            Group1=dict(id=31090725, get=dict(cmd=device.QueryInstanceGroup1), set=dict(cmd=device.SetInstanceGroup1), state=int),
            Group2=dict(id=31090726, get=dict(cmd=device.QueryInstanceGroup2), set=dict(cmd=device.SetInstanceGroup2), state=int),
            EventScheme=dict(id=31090727, set=dict(cmd=device.SetEventScheme), get=dict(cmd=device.QueryEventScheme), state=str, variants=["esIndexAndType", "esAddressAndType", "esAddressAndIndex", "esLowestGroupAndType", "esPrimaryGroupAndType"], def_value='esIndexAndType'),
            EventPriority=dict(id=31090728, get=dict(cmd=device.QueryEventPriority), set=dict(cmd=device.SetEventPriority), state=int),
            EventFilter=dict(id=31090729, set=dict(cmd=device.SetEventFilter), get=dict(cmd=device.QueryEventFilterL), state=list, value=str, mask=["occupiedEvents", "vacantEvents", "repeatEvents", "movementEvents", "noMovementEvents"]),
            DeadTime=dict(id=31090730, get=dict(cmd=device_presence.QueryDeadTime), set=dict(cmd=device_presence.SetDeadTime), state=int),
            ReportTime=dict(id=31090731, get=dict(cmd=device_presence.QueryReportTime), set=dict(cmd=device_presence.SetReportTime), state=int),
            HoldTime=dict(id=31090732, get=dict(cmd=device_presence.QueryHoldTime), set=dict(cmd=device_presence.SetHoldTime), state=int),
            Gtin=dict(id=31090741, get=dict(func='Gtin'), state=str),
            Serial=dict(id=31090742, get=dict(func='Serial'), state=str),
            GtinOem=dict(id=31090743, get=dict(func='GtinOem'), state=str),
            SerialOem=dict(id=31090744, get=dict(func='SerialOem'), state=str),
            FirmwareVersion=dict(id=31090745, get=dict(func='FirmwareVersion'), state=str),
            HardwareVersion=dict(id=31090746, get=dict(func='HardwareVersion'), state=str),
            InstancesNumber=dict(id=31090747, get=dict(cmd=device.QueryNumberOfInstances), state=int),
            InstanceIndex=dict(id=31090748, get=dict(func='GetInstanceIndex'), state=int),
            InstanceType=dict(id=31090749, get=dict(value=None), state=int),
            FeatureTypes=dict(id=31090750, get=dict(func='GetFeatureTypes'), state=int),
            Resolution=dict(id=31090751, get=dict(cmd=device.QueryResolution), state=int),
            Event=dict(id=31090761, state=int), #  (Last Event Data, 10bit)
            CurrentPresence=dict(id=31090762, get=dict(func='GetCurrentPresence'), state=int)
        ),
    ),
    Mercury=dict(
        Ready=dict(id=41010002, state=bool),
        MercuryElectricMeter=dict(
            Power=dict(id=41010101, get=dict(params=dict(phase=int, power_id=int), cmd=mercury.AUXPower)),
            Voltage=dict(id=41010102, get=dict(params=dict(phase=int), cmd=mercury.AUXVoltage)),
            Amperage=dict(id=41010103, get=dict(params=dict(phase=int), cmd=mercury.AUXAmperage)),
            Angle=dict(id=41010104, get=dict(params=dict(phase=int), cmd=mercury.AUXAngle)),
            Frequency=dict(id=41010105, get=dict(params=dict(phase=int), cmd=mercury.AUXFrequency)),
            PowerRate=dict(id=41010106, get=dict(params=dict(phase=int), cmd=mercury.AUXPowerRate)),
            StoredEnergyActiveStraight=dict(id=41010107, get=dict(params=dict(period=int, tariff=int, month=int), cmd=mercury.StoredEnergy)),
            StoredEnergyReactiveStraight=dict(id=41010108, get=dict(params=dict(period=int, tariff=int, month=int), cmd=mercury.StoredEnergy)),
            StoredEnergyActiveReverse=dict(id=41010109, get=dict(params=dict(period=int, tariff=int, month=int), cmd=mercury.StoredEnergy)),
            StoredEnergyReactiveReverse=dict(id=41010110, get=dict(params=dict(period=int, tariff=int, month=int), cmd=mercury.StoredEnergy)),
            StoredEnergyCombo=dict(id=41010111, get=dict(params=dict(period=int, tariff=int, month=int), cmd=mercury.StoredEnergyCombo)),
            Temperature=dict(id=41010112, get=dict(cmd=mercury.AUXTemp)),
            SerialDate=dict(id=41010113, get=dict(cmd=mercury.SerialCommand)),
            CurrentDateTime=dict(id=41010114, get=dict(cmd=mercury.CurrentDateTime)),
            OpenCloseTime=dict(id=41010115, get=dict(params=dict(item_id=int), cmd=mercury.OpenCloseTime)),
            Version=dict(id=41010116, get=dict(cmd=mercury.Version)),
            TransformRateVoltage=dict(id=41010117, set=dict(cmd=mercury.SetTransformRate), get=dict(cmd=mercury.TransformRate)),
            TransformRateAmperage=dict(id=41010118, set=dict(cmd=mercury.SetTransformRate), get=dict(cmd=mercury.TransformRate)),
            Info=dict(id=41010119)
        )
    ),
    Lom=dict(
        Ready=dict(id=41020001, state=bool),
        Profiles=dict(id=41020002, set=dict(func='SetProfiles'), state=list),
        AirBitStreetLamp={
            lom.const.BRIGHTNESS_LEVEL:     dict(id=41020011, set=dict(func='SetBrightnessLevel'), state=int),
            lom.const.CONFIG:               dict(set=dict(cmd=lom.commands.SetConfig), get=dict(cmd=lom.commands.GetConfig)),
            lom.const.CURRENT_PROFILE:      dict(set=dict(func='SetCurrentProfile')),
            # lom.const.TIME: dict(id=41020101, get=dict(params=dict(phase=int, power_id=int), cmd=mercury.AUXPower)),
            # lom.const.OPER_TIME: dict(id=41020102),
            # lom.const.CURRENT_PROFILE_ID: dict(id=41020106),
            # lom.const.TEMP: dict(id=41020107),
            # lom.const.TEMP_MIN: dict(id=41020108),
            # lom.const.TEMP_MAX: dict(id=41020109),
            # lom.const.ILLUMINATION: dict(id=41020110),
            # lom.const.TILT: dict(id=41020111),
            # lom.const.FLAGS: dict(id=41020112),
            # lom.const.DEVICE_SERIAL:                dict(id=41020113, c_id=1, size=4, get=lom.commands.GetConf),
            # lom.const.DEVICE_MODEL_ID:              dict(id=41020114, c_id=2, size=1, get=lom.commands.GetConf),
            # lom.const.FIRMWARE_VERSION:             dict(id=41020115, c_id=3, size=1, get=lom.commands.GetConf),
            # lom.const.ACTIVATION_STATUS:            dict(id=41020116, c_id=4, size=1, get=lom.commands.GetConf),
            # lom.const.DEVICE_USER_PASS:             dict(id=41020117, c_id=5, size=4, get=lom.commands.GetConf, state=str),
            # lom.const.LORAWAN_MC_ADDRESS_GROUP1:    dict(id=41020118, c_id=6, size=4, get=lom.commands.GetConf),
            # lom.const.LORAWAN_DEV_EUI:              dict(id=41020119, c_id=7, size=8, get=lom.commands.GetConf),
            # lom.const.LORAWAN_NETWORK_ID:           dict(id=41020120, c_id=257, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_REGION:               dict(id=41020121, c_id=258, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, variants=['AS923', 'AU915', 'CN470', 'CN779', 'EU433', 'EU868', 'KR920', 'IN865', 'US915', 'RU864']),
            # lom.const.LORAWAN_CLASS:                dict(id=41020122, c_id=259, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, variants=['CLASS_A', 'CLASS_B', 'CLASS_C']),
            # lom.const.LORAWAN_NB_TRIALS:            dict(id=41020123, c_id=260, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_TX_DATARATE:          dict(id=41020124, c_id=261, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, format='DT{}'),
            # lom.const.LORAWAN_IS_PUBLIC_NETWORK:    dict(id=41020125, c_id=262, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            # lom.const.LORAWAN_ADR_ENABLED:          dict(id=41020126, c_id=263, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            # lom.const.LORAWAN_MULTICAST_GROUP1_EN:  dict(id=41020127, c_id=264, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            # lom.const.LORAWAN_MULTICAST_GROUP2_EN:  dict(id=41020128, c_id=281, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            # lom.const.LORAWAN_MULTICAST_GROUP3_EN:  dict(id=41020129, c_id=282, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            # lom.const.LORAWAN_MULTICAST_GROUP4_EN:  dict(id=41020130, c_id=283, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            # lom.const.LORAWAN_REJOIN_INTERVAL_SEC:  dict(id=41020131, c_id=265, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_TIMESYNC_INTERVAL_SEC:dict(id=41020132, c_id=266, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_SEND_INTERVAL_SEC:    dict(id=41020133, c_id=267, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_NWKCHECK_INTERVAL_SEC:dict(id=41020134, c_id=268, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_NWKCHECK_ATTEMPTS:    dict(id=41020135, c_id=269, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LORAWAN_CONFIRMATION_ENABLED: dict(id=41020136, c_id=270, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, state=bool),
            #
            # lom.const.LATITUDE:                     dict(id=41020137, c_id=271, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.LONGITUDE:                    dict(id=41020138, c_id=272, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.ALTITUDE:                     dict(id=41020139, c_id=273, size=2, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.ANGLE_THRESHOLD:              dict(id=41020140, c_id=274, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.ACCEL_SENSITIVITY:            dict(id=41020141, c_id=275, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf, variants=['Off', 'Low', 'Middle', 'High']),
            # lom.const.DALI_SCAN_INTERVAL:           dict(id=41020142, c_id=276, size=2, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.ANSWER_RAND_INTERVAL:         dict(id=41020143, c_id=277, size=4, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # lom.const.DALI_ACTIVE_GLOBAL_PROFILE:   dict(id=41020144, c_id=278, size=1, get=lom.commands.GetConf, set=lom.commands.SetConf),
            # 'TempDimming':                          dict(id=41020144, set=lom.commands.SetDimmingLevel)
        }
    )
)
