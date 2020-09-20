import argparse
import logging
import os
import sys

import yaml

DUMPED = True
PROJECT_ID = 'PROJECT_ID'
DOUBLE = 'double'
DUMP = 'DUMP'
LOG_LEVEL = 'LOG_LEVEL'
DEBUGGING_HOST = 'DEBUGGING_HOST'
BROKER_USERNAME = 'BROKER_USERNAME'
BROKER_PASSWORD = 'BROKER_PASSWORD'
BROKER_HOST = 'BROKER_HOST'
BROKER_PORT = 'BROKER_PORT'
KILL_ITEM = 'remove item from local dump file'
CALENDAR = 'CALENDAR'
TASKS = 'TASKS'
TRIGGERS = 'TRIGGERS'


"""CONFIG"""


def read_conf(owner, file_name):
    res = yaml.load(open(file_name, "r"), Loader=yaml.FullLoader)
    if res:
        for name in res.keys():
            if name == 'include' and isinstance(res[name], list):
                for inc_item in res[name]:
                    read_conf(owner, inc_item)
            else:
                owner[name] = res[name]
    else:
        raise


def generate_dump():
    if config[DUMP]:
        dump_file = config[DUMP]
        if not os.path.exists(os.path.dirname(dump_file)):
            os.mkdir(os.path.dirname(dump_file))

        if os.path.exists(dump_file):
            try:
                read_conf(dump, config[DUMP])
            except BaseException as ex:
                try:
                    read_conf(dump, f'{config[DUMP]}_{DOUBLE}')
                except:
                    logging.warning('DUMP reading exception: {}'.format(ex))
        elif os.path.exists(f'{config[DUMP]}_{DOUBLE}'):
            try:
                read_conf(dump, f'{config[DUMP]}_{DOUBLE}')
            except BaseException as ex:
                logging.warning('DUMP_double reading exception: {}'.format(ex))
    else:
        dump_file = os.path.basename(args.config)
        if '.' in dump_file:
            dump_file = dump_file[:dump_file.rindex('.')]
        dump_path = os.path.dirname(args.config) + '/' + dump_file + os.path.extsep + 'dump'
        logging.warning('!!! DUMP file is not provided. Set temp DUMP in {}!!!'.format(dump_path))
        config[DUMP] = dump_path


"""DUMP"""


def get_dump(entity_type, entity_id, funit_type):
    if entity_type in dump:
        if entity_id in dump[entity_type]:
            if funit_type in dump[entity_type][entity_id]:
                return True, dump[entity_type][entity_id][funit_type]

    return False, None


def get_dump_entity(entity):
    if entity.__class__.__name__ in dump:
        if entity.id in dump[entity.__class__.__name__]:
            return dump[entity.__class__.__name__][entity.id]

    return dict()


def set_dump(entity, funit_type, value):
    if DUMP not in config:
        logging.warning('Конфиг не содержит дампа-файла!!!')
        return
    _override = False
    if entity.__class__.__name__ not in dump:
        _override = True
        dump[entity.__class__.__name__] = dict()
    if entity.id not in dump[entity.__class__.__name__]:
        _override = True
        dump[entity.__class__.__name__][entity.id] = dict()
    if funit_type not in dump[entity.__class__.__name__][entity.id] or dump[entity.__class__.__name__][entity.id][funit_type] != value:
        _override = True
        if value == KILL_ITEM:
            if funit_type in dump[entity.__class__.__name__][entity.id]:
                dump[entity.__class__.__name__][entity.id].pop(funit_type)
        else:
            dump[entity.__class__.__name__][entity.id][funit_type] = value

    if _override:
        with open(config[DUMP], 'w') as outfile:
            yaml.dump(dump, outfile, default_flow_style=False)


def on_exit():
    if DUMP in config:
        try:
            with open(config[DUMP], 'w') as f:
                yaml.dump(dump, f)
            with open(f'{config[DUMP]}_{DOUBLE}', 'w') as f:
                yaml.dump(dump, f)
        except BaseException as ex:
            logging.error(ex)


config = dict()
dump = dict()


parser = argparse.ArgumentParser(description='Spread services launcher')
parser.add_argument('-c', '--config', type=str, help='provide a config file path')
args = parser.parse_args()

if isinstance(args.config, str) and os.path.isfile(args.config):
    try:
        read_conf(config, args.config)
    except BaseException as ex:
        logging.exception(ex)

    if LOG_LEVEL not in config:
        config[LOG_LEVEL] = logging.DEBUG
else:
    print('ATTENTION: Settings file not set. Please provide some settings file. Breaked...')
    sys.exit(0)


"""LOGGING"""
log_level = int(config[LOG_LEVEL]) if LOG_LEVEL in config and str.isdigit(str(config[LOG_LEVEL])) else logging.INFO
log_format = u'%(levelname)-8s [%(asctime)s]  %(message)s'
logging.basicConfig(format=log_format, level=log_level, stream=sys.stdout)

