#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: singleton.py
Author: dutyu
Date: 2019/01/22 16:01:23
Brief: singleton
"""
import functools
import threading

__all__ = ["synchronized", "singleton"]


def synchronized(func):
    """ 同步装饰器"""

    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


def singleton(cls):
    """ 单例装饰器, 引用自
    https://wiki.python.org/moin/PythonDecoratorLibrary#Singleton"""

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