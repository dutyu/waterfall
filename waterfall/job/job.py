#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: job.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: job
"""
from abc import abstractmethod
from multiprocessing.managers import BaseProxy
from types import FunctionType
from typing import Iterator

from waterfall.config.config import Config
from waterfall.logger import Logger, monitor
from waterfall.utils.singleton import synchronized
from waterfall.utils.validate import validate, validate2


class Job(object):
    def __init__(self, job_id, name, config, first_step):
        validate(name, str)
        validate(config, Config)
        validate(first_step, FirstStep)
        self._id = job_id
        self._name = name
        self._config = config
        self._first_step = first_step

    def set_config(self, config):
        validate(config, Config)
        self._config = config

    def get_config(self):
        return self._config

    def get_step(self):
        return self._first_step

    def get_name(self):
        return self._name

    def get_id(self):
        return self._id

    @abstractmethod
    def stimulate(self):
        pass

    @staticmethod
    @abstractmethod
    def build():
        pass

    def validate(self, job_state):
        for step_name in job_state.keys():
            step_info = job_state[step_name]
            if step_info.get('err_cnt') > 0:
                return False
        return True


class Step(object):
    def __init__(self, runner, container_type='thread',
                 init_parallelism=2, max_parallelism=8):
        validate(init_parallelism, int)
        validate(max_parallelism, int)
        validate2(init_parallelism, max_parallelism,
                  func=lambda v1, v2: 0 < v1 <= v2,
                  err_msg='init_parallelism should be gt then 0 '
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

    def set_next_step(self, next_step):
        validate(next_step, Step)
        self._next_step = next_step
        self._next_step._seq_no = self._seq_no + 1
        return self._next_step

    def is_last_step(self):
        return self._next_step is None

    def get_parallelism(self):
        return self._init_parallelism

    def get_pool_type(self):
        return self._pool_type

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

    def get_name(self):
        return 'step' + str(self._seq_no)

    def get_seq_no(self):
        return self._seq_no

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
    @monitor
    def run(self, step_name, config, param, monitor_queue,
            res_queue, exit_flag):
        validate(step_name, str)
        validate(config, Config)
        validate(monitor_queue, BaseProxy)
        validate(res_queue, BaseProxy, None)
        if exit_flag.value:
            Logger().info_logger \
                .warn('receive an exit signal, return now !')
            return
        try:
            res = self._run(config, param, exit_flag)
        except Exception as e:
            Logger().error_logger.exception(e)
            self._fail(exit_flag, step_name, monitor_queue)
        else:
            if exit_flag.value:
                Logger().info_logger \
                    .warn('receive an exit signal, return now !')
                return
            try:
                self._handle_result(step_name, res,
                                    res_queue, monitor_queue)
            except Exception as e:
                Logger().error_logger.exception(e)
                self._fail(exit_flag, step_name, monitor_queue)
            else:
                self._suc(exit_flag, step_name, monitor_queue)

    @abstractmethod
    def _run(self, config, param, exit_flag):
        pass

    @staticmethod
    def _fail(exit_flag, step_name, monitor_queue):
        try:
            msg = {'res': 'f', 'step': step_name,
                   'type': 'c'}
            monitor_queue.put(msg)
        except Exception as e:
            exit_flag.value = 1
            Logger().error_logger.exception(e)

    @staticmethod
    def _suc(exit_flag, step_name, monitor_queue):
        try:
            msg = {'res': 's', 'step': step_name,
                   'type': 'c'}
            monitor_queue.put(msg)
        except Exception as e:
            exit_flag.value = 1
            Logger().error_logger.exception(e)

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
