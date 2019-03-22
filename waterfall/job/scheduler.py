#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: scheduler.py
Author: dutyu
Date: 2019/01/29 11:48:23
Brief: scheduler
"""
import sys
from multiprocessing import Manager
from typing import Iterator

from waterfall.config.config import Config
from waterfall.job.container import Container
from waterfall.job.job import Job, FirstStep
from waterfall.job.monitor import JobMonitor
from waterfall.logger import Logger
from waterfall.queue import QueueFactory
from waterfall.utils.decorators import singleton
from waterfall.utils.validate import validate, at_most


@singleton
class JobScheduler(object):
    def __init__(self, config=Config()):
        validate(config, Config)
        self._config = config
        """0; 正常, 非0: 异常"""
        self._exit_flag = Manager().Value('b', 0)
        self._job_list = []
        self._monitor_list = []
        self._container_list = []
        self._state = 'init'
        self._queue_factory = QueueFactory()

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

    def close(self):
        self._join()
        self._state = 'closed'
        sys.exit(self._exit_flag.value)

    def _join(self):
        for container in self._container_list:
            container.join()
        for monitor in self._monitor_list:
            monitor.join()

    def _start(self):
        if self._state != 'ready':
            raise RuntimeError('wrong state of container , '
                               'container not ready !')
        self._state = 'running'
        Logger().debug_logger \
            .debug('start container ! config: {%s}',
                   self._config)

        for job in self._job_list:
            self._queue_factory.register_queues(job)
            job_monitor = JobMonitor(self._config)
            monitor_queue = self._queue_factory \
                .get_monitor_queue(job)
            job_monitor.register(job, monitor_queue,
                                 self._exit_flag)
            job_monitor.start()
            self._monitor_list.append(job_monitor)
            step = job.get_step()
            while step:
                if isinstance(step, FirstStep):
                    input_data = job.stimulate()
                    at_most(2 ** 20, input_data,
                            'the return value of the job\'s '
                            'stimulate method is too large !')
                    input_queue = self._queue_factory \
                        .get_input_queue(job, step)
                    if isinstance(input_data, Iterator):
                        input_data = job.stimulate()
                    for item in input_data:
                        input_queue.put(item)
                    step.almost_done()
                    step.set_task_cnt(input_queue.qsize())
                else:
                    input_queue = res_queue
                res_queue = self._queue_factory \
                    .get_res_queue(job, step)
                container = Container(step, job.get_config(), input_queue,
                                      res_queue, monitor_queue,
                                      self._exit_flag)
                container.start()
                self._container_list.append(container)
                step = step.get_next_step()
