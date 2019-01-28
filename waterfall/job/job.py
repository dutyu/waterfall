#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: config.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: job
"""
import json
import threading
import time
import weakref
from abc import abstractmethod
from multiprocessing import Manager
from multiprocessing.pool import Pool, ThreadPool
from types import FunctionType
from typing import Iterator

from waterfall.config.config import Config
from waterfall.logger import Logger, monitor
from waterfall.utils.decorators import singleton, synchronized
from waterfall.utils.validate import validate, validate2


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
        validate(step, Step)
        threading.Thread.__init__(self)
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
                self._run(pool)
                pool.close()
                pool.join()
                next_step = self._step.get_next_step()
                if next_step:
                    next_step.almost_done()
                return
            else:
                self._run(pool)

    def _run(self, pool):
        while not self._input_queue.empty():
            msg = self._input_queue.get()
            pool.apply_async(self._step.get_runner().run,
                             (self._step.get_name(), msg,
                              self._monitor_queue,
                              self._res_queue,
                              self._err_flag))
            self._input_queue.task_done()

    def _get_pool(self):
        parallelism = self._step.get_parallelism()
        pool_type = self._step.get_pool_type()
        pool = Pool(parallelism, self._step.get_init_func()) \
            if pool_type == 'process' \
            else ThreadPool(parallelism)
        return pool


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
        validate(runner, Step.Runnable)
        self._seq_no = -1
        self._init_parallelism = init_parallelism
        self._max_parallelism = max_parallelism
        self._pool_type = container_type
        self._next_step = None
        self._runner = runner
        self._almost_done = False
        self._init_func = None

    @synchronized
    def set_next_step(self, next_step):
        validate(next_step, Step)

        def _alert():
            raise RuntimeError('next_step has been destroyed !')

        self._next_step = weakref.proxy(next_step, _alert)
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

    def get_name(self):
        return 'step' + str(self._seq_no)

    def set_init_func(self, func):
        validate(func, FunctionType)
        self._init_func = func

    def get_init_func(self):
        """使用进程池的时候,如果需要初始化,才调用"""
        return self._init_func

    class Runnable(object):

        @abstractmethod
        def _run(self, param, err_flag):
            pass

        @staticmethod
        def _notice_fail(step_name, monitor_queue):
            msg = {'res': 'f', 'step': step_name,
                   'type': 'consume'}
            monitor_queue.put(msg)

        @staticmethod
        def _notice_suc(step_name, monitor_queue):
            msg = {'res': 's', 'step': step_name,
                   'type': 'consume'}
            monitor_queue.put(msg)

        @staticmethod
        def _notice_produce(step_name, monitor_queue, cnt=1):
            Step.Runnable._notice_suc(step_name, monitor_queue)
            validate(cnt, int)
            msg = {'step': step_name, 'type': 'produce',
                   'cnt': cnt}
            monitor_queue.put(msg)

        def run(self, step_name, param, monitor_queue,
                res_queue, err_flag):
            if err_flag.value:
                Logger().info_logger \
                    .warn('receive a error signal, return now !')
                return
            try:
                res = self._run(param, err_flag)
            except Exception as e:
                Logger().error_logger.exception(e)
                self._notice_fail(step_name, monitor_queue)
            else:
                if err_flag.value:
                    Logger().info_logger \
                        .warn('receive a error signal, return now !')
                    return
                if res_queue:
                    if isinstance(res, Iterator):
                        try:
                            cnt = 0
                            for item in res:
                                if item is not None:
                                    res_queue.put(item)
                                    cnt += 1
                            self._notice_produce(
                                step_name, monitor_queue, cnt)
                        except Exception as e:
                            Logger().error_logger.exception(e)
                            self._notice_fail(
                                step_name, monitor_queue)
                    elif res is not None:
                        res_queue.put(res)
                        self._notice_produce(
                            step_name, monitor_queue)
                    else:
                        self._notice_suc(
                            step_name, monitor_queue)
                else:
                    self._notice_suc(
                        step_name, monitor_queue)


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


class JobMonitor(threading.Thread):
    def __init__(self, config=Config()):
        validate(config, Config)
        threading.Thread.__init__(self)
        self._config = config
        self._job = None
        self._err_flag = None
        self._queue = None
        self._job_info = None
        self._state = 'init'

    def register(self, job, monitor_queue, err_flag):
        validate(job, Job)
        self._job = job
        self._err_flag = err_flag
        self._queue = monitor_queue
        self._job_info = self._init_job_info()
        self._state = 'ready'

    def _init_job_info(self):
        job_info = {}
        step = self._job.get_step()
        while step:
            job_info[step.get_name()] = {'consume_cnt': 0,
                                         'produce_cnt': 0,
                                         'err_cnt': 0}
            step = step.get_next_step()
        return job_info

    def _print_progress(self):
        step = self._job.get_step()
        pre_step = None
        while step:
            step_name = step.get_name()
            step_info = self._job_info[step_name]
            consume_cnt = step_info.get('consume_cnt')
            err_cnt = step_info.get('err_cnt')
            if isinstance(step, FirstStep):
                task_cnt = step.get_task_cnt()
            else:
                pre_step_info = self._job_info[pre_step.get_name()]
                task_cnt = pre_step_info.get('produce_cnt')
            progress = 1 if task_cnt == 0 else consume_cnt / task_cnt
            err_rate = 0 if task_cnt == 0 else err_cnt / task_cnt
            progress_info = 'step: {:s}, progress: {:.2%}, ' \
                            'err_rate: {:.2%} consume_cnt: {:d}, ' \
                            'task_cnt: {:d}, err_cnt: {:d}, ' \
                .format(step_name, progress, err_rate,
                        consume_cnt, task_cnt, err_cnt)
            Logger().progress_logger.info(progress_info)
            pre_step = step
            step = step.get_next_step()

    def _refresh_progress(self):
        while not self._queue.empty():
            msg = self._queue.get()
            step_info = self._job_info[msg.get('step')]
            if msg.get('type') == 'produce':
                step_info['produce_cnt'] += msg.get('cnt')
            elif msg.get('type') == 'consume':
                res = msg.get('res')
                if res == 'f':
                    step_info['err_cnt'] += 1
                else:
                    step_info['consume_cnt'] += 1
            self._queue.task_done()

    def run(self):
        if self._state != 'ready':
            raise RuntimeError('wrong state of monitor, not ready !')
        try:
            while time.sleep(10) or True:
                if self._err_flag.value:
                    return
                self._refresh_progress()
                self._print_progress()
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

    def add_job(self, job):
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
                    step.set_task_cnt(input_queue.qsize())
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
