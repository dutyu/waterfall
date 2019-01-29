#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: config.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: job
"""
import weakref
from abc import abstractmethod
from multiprocessing.managers import BaseProxy
from types import FunctionType
from typing import Iterator

from waterfall.config.config import Config
from waterfall.logger import Logger
from waterfall.utils.decorators import synchronized
from waterfall.utils.validate import validate, validate2


class Job(object):
    def __init__(self, name, config, first_step):
        validate(name, str)
        validate(config, Config)
        validate(first_step, FirstStep)
        self._name = name
        self._config = config
        self._first_step = first_step

    def get_config(self):
        return self._config

    def get_step(self):
        return self._first_step

    def get_name(self):
        return self._name

    @abstractmethod
    def stimulate(self):
        pass


class Step(object):
    def __init__(self, runner, container_type='thread',
                 init_parallelism=2, max_parallelism=8):
        validate(init_parallelism, int)
        validate(max_parallelism, int)
        validate2(init_parallelism, max_parallelism,
                  func=lambda v1, v2: 0 < v1 <= v2,
                  err_msg='init_parallelism should gt then 0 '
                          'and lte then max_parallelism')
        validate(container_type, str)
        validate(runner, Runnable)
        self._seq_no = -1
        self._init_parallelism = init_parallelism
        self._max_parallelism = max_parallelism
        self._pool_type = container_type
        self._next_step = None
        self._runner = runner
        self._almost_done = False
        self._done = False
        self._init_func = None

    @synchronized
    def set_next_step(self, next_step):
        validate(next_step, Step)
        self._next_step = weakref.proxy(next_step)
        self._next_step._seq_no = self._seq_no + 1
        return self._next_step

    def is_last_step(self):
        return self._next_step is None

    def get_parallelism(self):
        return self._init_parallelism

    def get_pool_type(self):
        return self._pool_type

    @synchronized
    def get_next_step(self):
        return self._next_step

    def get_runner(self):
        return self._runner

    @synchronized
    def almost_done(self):
        self._almost_done = True

    @synchronized
    def get_almost_done(self):
        return self._almost_done

    @synchronized
    def get_done(self):
        return self._done

    @synchronized
    def set_done(self):
        self._done = True

    @synchronized
    def get_name(self):
        return 'step' + str(self._seq_no)

    def set_init_func(self, func):
        validate(func, FunctionType)
        self._init_func = func

    def get_init_func(self):
        """使用进程池的时候,如果需要初始化,才调用"""
        return self._init_func


class FirstStep(Step):
    def __init__(self, runner, container_type='thread',
                 init_parallelism=2, max_parallelism=8):
        Step.__init__(self, runner, container_type,
                      init_parallelism, max_parallelism)
        self._seq_no = 1
        self._task_cnt = 0

    def get_task_cnt(self):
        return self._task_cnt

    def set_task_cnt(self, task_cnt):
        self._task_cnt = task_cnt


class Runnable(object):
    @abstractmethod
    def _run(self, param, exit_flag):
        pass

    @staticmethod
    def _fail(step_name, monitor_queue):
        msg = {'res': 'f', 'step': step_name,
               'type': 'c'}
        monitor_queue.put(msg)

    @staticmethod
    def _suc(step_name, monitor_queue):
        msg = {'res': 's', 'step': step_name,
               'type': 'c'}
        monitor_queue.put(msg)

    @staticmethod
    def _produce(item, step_name,
                 res_queue, monitor_queue):
        res_queue.put(item)
        msg = {'step': step_name, 'cnt': 1,
               'type': 'p'}
        monitor_queue.put(msg)

    def _handle_result(self, step_name, res,
                       res_queue, monitor_queue):
        if res_queue:
            if isinstance(res, Iterator):
                for item in res:
                    self._produce(item, step_name,
                                  res_queue, monitor_queue)
            elif res is not None:
                self._produce(res, step_name,
                              res_queue, monitor_queue)

    def run(self, step_name, param, monitor_queue,
            res_queue, exit_flag):
        validate(step_name, str)
        validate(monitor_queue, BaseProxy)
        if exit_flag.value:
            Logger().info_logger \
                .warn('receive a exit signal, return now !')
            return
        try:
            res = self._run(param, exit_flag)
        except Exception as e:
            Logger().error_logger.exception(e)
            self._fail(step_name, monitor_queue)
        else:
            if exit_flag.value:
                Logger().info_logger \
                    .warn('receive a exit signal, return now !')
                return
            try:
                self._handle_result(step_name, res,
                                    res_queue, monitor_queue)
            except Exception as e:
                Logger().error_logger.exception(e)
                self._fail(step_name, monitor_queue)
            else:
                self._suc(step_name, monitor_queue)
