#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: monitor.py
Author: dutyu
Date: 2019/01/29 11:53:13
Brief: monitor
"""
import threading

import time
from multiprocessing.managers import BaseProxy

from waterfall.config.config import Config
from waterfall.job.job import Job, FirstStep
from waterfall.logger import Logger
from waterfall.utils.validate import validate


class JobMonitor(threading.Thread):
    _RULES_LINE = '_'.join(
        ('' for i in range(120)))

    def __init__(self, config=Config()):
        validate(config, Config)
        threading.Thread.__init__(self)
        self._config = config
        self._job = None
        self._exit_flag = None
        self._queue = None
        self._job_info = None
        self._state = 'init'
        self._start_ts = None

    def register(self, job, monitor_queue, exit_flag):
        validate(job, Job)
        validate(monitor_queue, BaseProxy)
        self._job = job
        self._exit_flag = exit_flag
        self._queue = monitor_queue
        self._job_info = self._init_job_info()
        self._state = 'ready'

    def run(self):
        if self._state != 'ready':
            raise RuntimeError('wrong state of monitor, '
                               'not ready !')
        self._start_ts = time.time()
        while not self._exit_flag.value \
                and not self._done() \
                and not time.sleep(10):
            self._refresh_progress()
            self._print_progress()

    def _init_job_info(self):
        job_info = {}
        step = self._job.get_step()
        while step:
            job_info[step.get_name()] = {'suc_cnt': 0,
                                         'produce_cnt': 0,
                                         'err_cnt': 0}
            step = step.get_next_step()
        return job_info

    def _done(self):
        if self._state == 'init':
            return False
        if self._state == 'done':
            return True
        step = self._job.get_step()
        while step:
            if not step.get_done():
                return False
            step = step.get_next_step()
        self._state = 'done'
        return True

    def _print_progress(self):
        step = self._job.get_step()
        pre_step = None
        Logger().progress_logger.info(self._RULES_LINE)
        while step:
            step_name = step.get_name()
            step_info = self._job_info[step_name]
            suc_cnt = step_info.get('suc_cnt')
            err_cnt = step_info.get('err_cnt')

            if isinstance(step, FirstStep):
                task_cnt = step.get_task_cnt()
            else:
                pre_step_info = self._job_info \
                    .get(pre_step.get_name())
                task_cnt = pre_step_info.get('produce_cnt')

            progress = 0 if task_cnt == 0 \
                else (suc_cnt + err_cnt) / task_cnt
            err_rate = 0 if task_cnt == 0 \
                else err_cnt / task_cnt
            progress_info = 'job: {:s}, step: {:s}, ' \
                            'progress: {:.2%}, ' \
                            'err_rate: {:.2%}, ' \
                            'suc_cnt: {:d}, ' \
                            'task_cnt: {:d}, ' \
                            'err_cnt: {:d}, ' \
                            'cost: {:.2f}' \
                .format(self._job.get_name(),
                        step_name,
                        progress,
                        err_rate,
                        suc_cnt,
                        task_cnt,
                        err_cnt,
                        time.time() - self._start_ts)
            step_info['progress'] = progress
            Logger().progress_logger.info(progress_info)
            pre_step = step
            step = step.get_next_step()

    def _refresh_progress(self):
        c_cnt = 0
        while not self._queue.empty():
            msg = self._queue.get()
            step_info = self._job_info[msg.get('step')]
            if msg.get('type') == 'p':
                step_info['produce_cnt'] += msg.get('cnt')
            elif msg.get('type') == 'c':
                res = msg.get('res')
                if res == 'f':
                    step_info['err_cnt'] += 1
                else:
                    step_info['suc_cnt'] += 1
            self._queue.task_done()
            c_cnt += 1
            if c_cnt >= 1000:
                self._print_progress()
                c_cnt = 0
