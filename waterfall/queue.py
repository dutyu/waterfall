#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: queue.py
Author: dutyu
Date: 2019/02/11 18:21:35
Brief: queue
"""
import random
from multiprocessing import Manager
from multiprocessing.managers import BaseManager

from waterfall.config.config import Config
from waterfall.job.job import Job, Step
from waterfall.logger import Logger
from waterfall.utils.singleton import singleton
from waterfall.utils.validate import validate

__all__ = ["QueueFactory"]


@singleton
class QueueFactory(object):
    class QueueManager(BaseManager):
        pass

    def __init__(self, config=Config()):
        self._config = config
        self._manager_cls = self.QueueManager
        self._managers = {}
        self._scheduler_manager = None
        self._queues = {}
        self._port = random.randint(5000, 6000)

    def register_scheduler_queue(self, port=5011):
        self._manager_cls.register('get_scheduler_queue',
                                   callable=_queue_producer_func)
        manager = self._manager_cls(address=('127.0.0.1', port))
        manager.start()
        self._scheduler_manager = manager

    def get_scheduler_queue(self):
        func = getattr(self._scheduler_manager, 'get_scheduler_queue')
        return func()

    def register_job_queues(self, job):
        validate(job, Job)
        job_id = str(job.get_id()).replace('-', '')
        monitor_func_str = 'monitor' + job_id
        self._manager_cls.register(monitor_func_str,
                                   callable=_queue_producer_func)
        step = job.get_step()
        while step:
            step_seq_no = str(step.get_seq_no())
            input_func_str = 'input' + job_id + '_' + step_seq_no
            self._manager_cls.register(input_func_str,
                                       callable=_queue_producer_func)
            step = step.get_next_step()
        manager = self._manager_cls(address=('127.0.0.1', self._port))
        manager.start()
        self._managers[job_id] = manager

    def get_input_queue(self, job, step, ip=''):
        validate(step, Step)
        validate(job, Job)
        job_id = str(job.get_id()).replace('-', '')
        step_seq_no = str(step.get_seq_no())
        input_func_str = 'input' + job_id + '_' + step_seq_no
        if self._queues.get(input_func_str) is None:
            queue = self._get_queue(job_id, input_func_str, ip)
            self._queues[input_func_str] = queue
        return self._queues.get(input_func_str)

    def get_res_queue(self, job, step, ip=''):
        validate(step, Step)
        validate(job, Job)
        if step.is_last_step():
            return None
        next_step = step.get_next_step()
        return self.get_input_queue(job, next_step, ip)

    def get_monitor_queue(self, job, ip=''):
        validate(job, Job)
        job_id = str(job.get_id()).replace('-', '')
        monitor_func_str = 'monitor' + job_id
        if self._queues.get(monitor_func_str) is None:
            queue = self._get_queue(job_id, monitor_func_str, ip)
            self._queues[monitor_func_str] = queue
        return self._queues.get(monitor_func_str)

    def _get_queue(self, job_id, func_str, ip=''):
        if ip == '':
            manager = self._managers[job_id]
        else:
            manager = self._manager_cls(address=(ip, self._port))
            manager.connect()
        func = getattr(manager, func_str)
        return func()

    def shutdown(self):
        try:
            for manager_id in self._managers:
                self._managers[manager_id].join(timeout=1)
        except Exception as e:
            Logger().error_logger.exception(e)


def _queue_producer_func():
    return Manager().Queue(10000)
