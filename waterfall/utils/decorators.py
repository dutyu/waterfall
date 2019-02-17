#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: decorators.py
Author: dutyu
Date: 2019/01/22 16:01:23
Brief: decorators
"""
import threading
import functools

from waterfall.config.config import Config
from waterfall.job.job import Job, Runnable
from waterfall.utils.validate import validate


__all__ = ["synchronized", "singleton", "job", "step", "custom_jobs"]


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


custom_jobs = {}


def job(job_name, config=Config()):
    """ 自定义job装饰器"""

    validate(job_name, str)

    def wrapper(cls):
        validate(cls, Job)
        pass


def step(job_name, config=Config()):
    """ 自定义step装饰器"""

    validate(job_name, str)

    def wrapper(cls):
        validate(cls, Runnable)
        pass