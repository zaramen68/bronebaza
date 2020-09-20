import sys

from spread_core.tools.settings import config, DEBUGGING_HOST


def attach():
    try:
        import pydevd_pycharm
    except BaseException as ex:
        print('\n'.join([str(ex), 'Use for install:', 'pip3 install pydevd-pycharm~=193.6494.30']))
        sys.exit(-1)
    else:
        print('Wait for debugger attach from {}:21000'.format(config[DEBUGGING_HOST]))
        pydevd_pycharm.settrace(config[DEBUGGING_HOST], port=21000, stdoutToServer=True, stderrToServer=True)