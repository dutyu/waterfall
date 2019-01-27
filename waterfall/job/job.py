#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: config.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: job
"""
import threading
import weakref
from abc import abstractmethod
from multiprocessing import Manager
from multiprocessing.pool import Pool, ThreadPool

from waterfall.config.config import Config
from waterfall.logger import Logger, monitor
from waterfall.utils.decorators import singleton, synchronized
from waterfall.utils.validate import validate


class Job(object):
    def __init__(self, config, first_step):
        validate(config, Config)
        validate(first_step, FirstStep)
        self._config = config
        self._first_step = first_step

    def get_config(self):
        return self._config

    def get_step(self):
        return self._first_step

    @abstractmethod
    def stimulate(self):
        pass


class Scheduler(threading.Thread):
    def __init__(self, step, input_queue, res_queue, monitor_queue, err_flag):
        validate(step, BoringStep)
        super(Scheduler, self).__init__()
        self._step = step
        self._input_queue = input_queue
        self._res_queue = res_queue
        self._monitor_queue = monitor_queue
        self._err_flag = err_flag

    @monitor
    def run(self):
        pool = self._get_pool()
        while True:
            if self._err_flag.value:
                pool.terminate()
                return
            if self._step.get_almost_done():
                self._schedule(pool)
                pool.close()
                pool.join()
                if self._step.get_next_step():
                    self._step.get_next_step().almost_done()
                return
            else:
                self._schedule(pool)

    def _schedule(self, pool):
        while not self._input_queue.empty():
            msg = self._input_queue.get()
            self._input_queue.task_done()
            pool.apply_async(self._step.get_runner().run,
                             (msg, self._res_queue,
                              self._monitor_queue,
                              self._err_flag))

    def _get_pool(self):
        parallelism = self._step.get_parallelism()
        pool_type = self._step.get_pool_type()
        pool = Pool(parallelism, self._step.process_init) \
            if pool_type == 'process' \
            else ThreadPool(parallelism)
        return pool


class BoringStep(object):
    class Runnable(object):
        @abstractmethod
        def run(self, params, res_queue, monitor_queue, err_flag):
            pass

    def __init__(self, parallelism, pool_type, runner):
        validate(parallelism, int)
        validate(pool_type, str)
        validate(runner, BoringStep.Runnable)
        self._parallelism = parallelism
        self._pool_type = pool_type
        self._next_step = None
        self._runner = runner
        self._almost_done = False

    def set_next_step(self, next_step):
        validate(next_step, BoringStep)

        def _alert():
            raise RuntimeError('next_step has been destroyed !')

        self._next_step = weakref.proxy(next_step, _alert)
        return self._next_step

    def is_last_step(self):
        return self._next_step is None

    def get_parallelism(self):
        return self._parallelism

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

    def process_init(self):
        """使用进程池的时候,如果需要初始化,才调用"""
        pass


class FirstStep(BoringStep):
    def __init__(self, parallelism, pool_type, runner):
        BoringStep.__init__(self, parallelism, pool_type, runner)


class JobMonitor(threading.Thread):
    def __init__(self, config=Config()):
        threading.Thread.__init__(self)
        validate(config, Config)
        self._config = config
        self._job = None
        self._err_flag = None
        self._queue = None

    def register(self, job, monitor_queue, err_flag):
        validate(job, Job)
        self._job = job
        self._err_flag = err_flag
        self._queue = monitor_queue

    def run(self):
        while True:
            try:
                if self._err_flag.value:
                    return
                while not self._queue.empty():
                    msg = self._queue.get()
                    self._queue.task_done()
                    # TODO
                    # do_something()
            except:
                pass


@singleton
class JobsContainer(object):
    def __init__(self, config=Config()):
        validate(config, Config)
        self._config = config
        self._err_flag = Manager().Value('b', False)
        self._job_list = []
        self._state = 'init'

    def get_state(self):
        return self._state

    def append_job(self, job):
        validate(job, Job)
        self._job_list.append(job)
        return self

    def set_ready(self):
        self._state = 'ready'
        return self

    def start(self):
        if self._state != 'ready':
            raise RuntimeError('wrong state of container , '
                               'container not ready !')
        self._state = 'running'
        Logger().debug_logger \
            .debug('start container ! config: {%s}', self._config)
        scheduler_list = []
        for job in self._job_list:
            job_monitor = JobMonitor(self._config)
            monitor_queue = Manager().Queue()
            job_monitor.register(job, monitor_queue, self._err_flag)
            job_monitor.setDaemon(True)
            job_monitor.start()
            step = job.get_step()
            while step:
                if isinstance(step, FirstStep):
                    input_queue = job.stimulate()
                    step.almost_done()
                else:
                    input_queue = res_queue
                res_queue = None if step.is_last_step() \
                    else Manager().Queue()
                scheduler = Scheduler(step, input_queue,
                                      res_queue, monitor_queue, self._err_flag)
                scheduler.start()
                scheduler_list.append(scheduler)
                step = step.get_next_step()

        for scheduler in scheduler_list:
            scheduler.join()
        if self._err_flag.value:
            raise RuntimeError('receive a error signal, exit !')

    def close(self):
        self._state = 'closed'
