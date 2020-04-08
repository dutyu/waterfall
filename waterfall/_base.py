#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: _base.py
Author: dutyu
Date: 2020/03/25 10:33:31
"""
import logging
import os
import threading
import time
import traceback
from threading import Condition
from typing import Any

from waterfall import _util

logger = logging.getLogger("waterfall")

IP = _util.get_host_ip()
PROVIDER_PORT = int(os.getenv('WATERFALL_PROVIDER_PORT', 6666))
CONSUMER_PORT = int(os.getenv('WATERFALL_CONSUMER_PORT', 7777))
WATERFALL_ENV = os.getenv('WATERFALL_ENV', 'default')
ZK_PATH = '/'.join(('', 'waterfall', WATERFALL_ENV))
DEFAULT_SEND_RETRY_TIMES = 3
DEFAULT_TIMEOUT_SEC = 10


class OfflineProvider(Exception):
    """The provider is offline now."""
    pass


class EmptyProvider(Exception):
    """Cluster doesn't have any provider."""
    pass


class BrokenProvider(Exception):
    """Provider has been broken."""
    pass


class BrokenRemoteQueue(Exception):
    """The RemoteQueue process has been broken."""
    pass


class ProviderTooBusy(Exception):
    """Worker is too busy to handle the request."""
    pass


class _RemoteTraceback(Exception):
    def __init__(self, tb):
        self.tb = tb

    def __str__(self):
        return self.tb


class ExceptionWithTraceback:
    def __init__(self, exc, tb):
        tb = traceback.format_exception(type(exc), exc, tb)
        tb = ''.join(tb)
        self.exc = exc
        self.tb = '\n"""\n%s"""' % tb

    def __reduce__(self):
        return _rebuild_exc, (self.exc, self.tb)


def _rebuild_exc(exc, tb):
    exc.__cause__ = _RemoteTraceback(tb)
    return exc


class WorkItem(object):
    def __init__(self, future, app_name, service, args, kwargs, provider_id, timeout):
        self.future = future
        self.app_name = app_name
        self.service = service
        self.args = args
        self.kwargs = kwargs
        self.provider_id = provider_id
        self.start_ts = time.time()
        self.timeout = timeout


class ResultItem(object):
    def __init__(self, work_id, exception=None, result=None):
        self.work_id = work_id
        self.exception = exception
        self.result = result


class CallItem(object):
    def __init__(self, work_id: str, port: int, service: str, args: Any, kwargs: Any):
        self.work_id = work_id
        self.service = service
        self.ip = IP
        self.port = port
        self.args = args
        self.kwargs = kwargs
        self.consumer_id = ':'.join((self.ip, str(self.port)))


class ProviderItem(object):
    def __init__(self, app_name: str, ip: str, port: int, meta_info: Any = None):
        self.app_name = app_name
        self.ip = ip
        self.port = int(port)
        self.meta_info = meta_info
        self.id = ':'.join((self.ip, str(self.port)))


class CountDownLatch(object):
    def __init__(self, count):
        self.count = count
        self.condition = Condition()

    def await(self, timeout=None):
        self.condition.acquire()
        try:
            while self.count > 0:
                return self.condition.wait(timeout)
        finally:
            self.condition.release()

    def count_down(self):
        self.condition.acquire()
        self.count -= 1
        if self.count <= 0:
            self.condition.notifyAll()
        self.condition.release()


class AtomicInteger(object):
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def inc(self):
        with self._lock:
            self._value += 1
            return self._value

    def dec(self):
        with self._lock:
            self.value -= 1
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value
