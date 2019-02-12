#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: queue.py
Author: dutyu
Date: 2019/02/11 18:21:35
Brief: queue
"""
import queue
from multiprocessing.managers import BaseManager

from waterfall.config.config import Config
from waterfall.job.job import Job, Step
from waterfall.utils.decorators import singleton
from waterfall.utils.validate import validate


@singleton
class QueueFactory(object):
    class QueueManager(BaseManager):
        pass

    def __init__(self, config=Config()):
        self._config = config
        self._manager_cls = self.QueueManager
        self._managers = {}

    def register_queues(self, job, port=5000):
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
        manager = self._manager_cls(address=('', port), authkey=b'abc')
        manager.start()
        self._managers[job_id] = manager

    def get_input_queue(self, job, step, ip='', port=5000):
        validate(step, Step)
        validate(job, Job)
        job_id = str(job.get_id()).replace('-', '')
        step_seq_no = str(step.get_seq_no)
        input_func_str = 'input' + job_id + '_' + step_seq_no
        return self._get_queue(job_id, input_func_str, ip, port)

    def get_res_queue(self, job, step, ip='', port=5000):
        validate(step, Step)
        validate(job, Job)
        job_id = str(job.get_id()).replace('-', '')
        next_step = step.get_next_step()
        return self.get_input_queue(job_id, next_step, ip, port)

    def get_monitor_queue(self, job, ip='', port=5000):
        validate(job, Job)
        job_id = str(job.get_id()).replace('-', '')
        monitor_func_str = 'monitor' + job_id
        return self._get_queue(job_id, monitor_func_str, ip, port)

    def _get_queue(self, job_id, func_str, ip='', port=5000):
        if ip == '':
            manager = self._managers[job_id]
        else:
            manager = self._manager_cls(address=(ip, port), authkey=b'abc')
            manager.connect()
        func = getattr(manager, func_str)
        return func()


def _queue_producer_func():
    return queue.Queue(10000)
