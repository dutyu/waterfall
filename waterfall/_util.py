#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: _util.py
Author: dutyu
Date: 2020/04/08 19:05:24
"""
import functools
import socket


def get_host_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def singleton(cls):
    """
    Reference: https://wiki.python.org/moin/PythonDecoratorLibrary#Singleton
    """

    cls.__new_original__ = cls.__new__

    @functools.wraps(cls.__new__)
    def singleton_new(_cls_, *args, **kw):
        it = _cls_.__dict__.get('__it__')
        if it is not None:
            return it
        _cls_.__it__ = it = _cls_.__new_original__(_cls_)
        it.__init_original__(*args, **kw)
        return it

    cls.__new__ = singleton_new
    cls.__init_original__ = cls.__init__
    cls.__init__ = object.__init__

    return cls
