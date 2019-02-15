#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: container.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: container
"""
import threading
from multiprocessing.managers import BaseProxy
from multiprocessing.pool import Pool, ThreadPool

from waterfall.job.job import Step
from waterfall.logger import monitor, Logger
from waterfall.utils.validate import validate


class Container(threading.Thread):
    def __init__(self, step, input_queue, res_queue,
                 monitor_queue, exit_flag):
        validate(step, Step)
        validate(input_queue, BaseProxy)
        validate(res_queue, BaseProxy, None)
        threading.Thread.__init__(self)
        self._step = step
        self._input_queue = input_queue
        self._res_queue = res_queue
        self._monitor_queue = monitor_queue
        self._exit_flag = exit_flag

    @monitor
    def run(self):
        pool = self._get_pool()
        while True:
            if self._exit_flag.value:
                try:
                    pool.terminate()
                except Exception as e:
                    Logger().error_logger.exception(e)
                return
            if self._step.get_almost_done():
                self._run(pool)
                pool.close()
                pool.join()
                self._step.set_done()
                next_step = self._step.get_next_step()
                if next_step:
                    next_step.almost_done()
                return
            else:
                self._run(pool)

    def _run(self, pool):
        while not self._input_queue.empty() and \
                not self._exit_flag.value:
            msg = self._input_queue.get()
            pool.apply_async(self._step.get_runner().run,
                             (self._step.get_name(), msg,
                              self._monitor_queue,
                              self._res_queue,
                              self._exit_flag))

    def _get_pool(self):
        parallelism = self._step.get_parallelism()
        pool_type = self._step.get_pool_type()
        pool = Pool(parallelism,
                    self._step.get_init_func()) \
            if pool_type == 'process' \
            else ThreadPool(parallelism)
        return pool
