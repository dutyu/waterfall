#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: container.py
Author: dutyu
Date: 2019/01/29 11:48:23
Brief: container
"""
import sys
from multiprocessing import Manager
from typing import Iterable
import cardinality

from waterfall.config.config import Config
from waterfall.job.job import Job, FirstStep
from waterfall.job.monitor import JobMonitor
from waterfall.job.schedule import Scheduler
from waterfall.logger import Logger
from waterfall.utils.decorators import singleton
from waterfall.utils.validate import validate


@singleton
class JobsContainer(object):
    def __init__(self, config=Config()):
        validate(config, Config)
        self._config = config
        """0; 正常, 非0: 异常"""
        self._exit_flag = Manager().Value('b', 0)
        self._job_list = []
        self._monitor_list = []
        self._scheduler_list = []
        self._state = 'init'

    def get_state(self):
        return self._state

    def add_job(self, job):
        validate(job, Job)
        self._job_list.append(job)
        return self

    def set_ready(self):
        self._state = 'ready'
        return self

    def start(self):
        try:
            self._start()
        except Exception as e:
            Logger().error_logger.exception(e)
            self._exit_flag.value = 1

    def _join(self):
        for scheduler in self._scheduler_list:
            scheduler.join()
        for monitor_t in self._monitor_list:
            monitor_t.join()

    def _start(self):
        if self._state != 'ready':
            raise RuntimeError('wrong state of container , '
                               'container not ready !')
        self._state = 'running'
        Logger().debug_logger \
            .debug('start container ! config: {%s}',
                   self._config)

        for job in self._job_list:
            job_monitor = JobMonitor(self._config)
            monitor_queue = Manager().Queue()
            job_monitor.register(job, monitor_queue,
                                 self._exit_flag)
            job_monitor.start()
            self._monitor_list.append(job_monitor)
            step = job.get_step()
            while step:
                if isinstance(step, FirstStep):
                    input_data = job.stimulate()
                    validate(input_data, Iterable,
                             err_msg='job\'s stimulate method '
                                     'must return a iterable value !')
                    if isinstance(input_data, Iterable) and \
                            not cardinality.at_most(2 ** 20, input_data):
                        raise RuntimeError('job\'s stimulate method '
                                           'produce too large items !')
                    input_queue = Manager().Queue()
                    input_data = job.stimulate()
                    for item in input_data:
                        input_queue.put(item)
                    step.almost_done()
                    step.set_task_cnt(cardinality.count(input_data))
                else:
                    input_queue = res_queue
                res_queue = None if step.is_last_step() \
                    else Manager().Queue(10000)
                scheduler = Scheduler(step, input_queue,
                                      res_queue, monitor_queue,
                                      self._exit_flag)
                scheduler.start()
                self._scheduler_list.append(scheduler)
                step = step.get_next_step()

    def close(self):
        self._join()
        self._state = 'closed'
        sys.exit(self._exit_flag.value)
