#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: logger.py
Author: dutyu
Date: 2018/03/02 16:59:03
Brief: logger
"""
import functools
import json
import logging
import logging.config
import time
import datetime

from waterfall.config.global_config import global_config, GlobalConfig
from waterfall.utils import fs
from waterfall.utils.const import const
from waterfall.utils.singleton import singleton


@singleton
class Logger(object):
    def __init__(self):
        self._conf_file = fs.path.join(const.ROOT_PATH,
                                       'logger.conf')
        logging.config.fileConfig(self._conf_file,
                                  defaults={'log_path': global_config.LOG_PATH,
                                            'seq': datetime.datetime.now().strftime('%Y-%m-%d+%H-%M-%S')})
        self._debug_logger = logging.getLogger('debugLogger')
        if not GlobalConfig.ENABLE_DEBUG:
            self._debug_logger.level = logging.CRITICAL
        self._info_logger = logging.getLogger('infoLogger')
        self._error_logger = logging.getLogger('errorLogger')
        self._monitor_logger = logging.getLogger('monitorLogger')
        self._io_logger = logging.getLogger('ioLogger')
        self._progress_logger = logging.getLogger('progressLogger')

    @property
    def debug_logger(self):
        return self._debug_logger

    @property
    def info_logger(self):
        return self._info_logger

    @property
    def error_logger(self):
        return self._error_logger

    @property
    def monitor_logger(self):
        return self._monitor_logger

    @property
    def io_logger(self):
        return self._io_logger

    @property
    def progress_logger(self):
        return self._progress_logger


def io(fn):
    """ io日志装饰器,打印方法输入输出"""

    def _filter(obj):
        type_tuple = (int, float, str, tuple, list, dict, set)
        return isinstance(obj, type_tuple)

    @functools.wraps(fn)
    def _wrapper(*args, **kwargs):
        exe = None
        res = None
        try:
            res = fn(*args, **kwargs)
        except Exception as e:
            exe = e
        try:
            Logger().io_logger.info(
                fn.__globals__.get('__name__') + '.' + fn.__name__ + '()' +
                ' input params: %s ||| output params: %s',
                json.dumps(tuple(filter(_filter, args)))
                + '|' + json.dumps(tuple(filter(_filter, kwargs.items()))),
                json.dumps(res if _filter(res) else None))
        except Exception as e:
            Logger().error_logger.exception(e)
        if exe:
            raise exe
        return res

    return _wrapper


def monitor(fn):
    """ monitor日志装饰器,打印方法耗时"""

    def _log_monitor(cost, flag):
        Logger().monitor_logger.info(
            fn.__globals__.get('__name__') + '.' +
            fn.__name__ + '()' + '-' +
            ('s' if flag else 'f') + '-' +
            'costs ' + str(cost) + ' ms')

    @functools.wraps(fn)
    def _wrapper(*args, **kwargs):
        start = time.time()
        exe = None
        res = None
        try:
            res = fn(*args, **kwargs)
        except Exception as e:
            exe = e
        if_success = True if exe is None else False
        cost_t = (time.time() - start) * 1000
        if not if_success:
            _log_monitor(cost_t, if_success)
            raise exe
        else:
            _log_monitor(cost_t, if_success)
        return res

    return _wrapper
