#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: test_job.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: test_job
"""
import random

import time

from waterfall.job.job import Runnable, Job
from waterfall.utils.decorators import job, step


@step('test_job')
class TestRunner(Runnable):
    def _run(self, params, exit_flag):
        if exit_flag.value:
            return
        # print("params : " + str(params))
        time.sleep(0.01)
        # raise RuntimeError('test')
        print("run finish !")
        return (i for i in range(0, 100))


@step('test_job')
class TestRunner2(Runnable):
    def _run(self, params, exit_flag):
        if exit_flag.value:
            return
        print("params : " + str(params))
        j = 0
        res = random.random()
        while j < 100000:
            res = (2 * 21 ** 3 / 3.231 + 2 ** 4 / 3211.23231
                   - 342342 * 32 + random.random()) % random.random()
            j += 1
        print("run finish ! res: {:.2f}".format(res))
        return res


@job('test_job')
class TestJob(Job):
    @staticmethod
    def _generator(res):
        i = 0
        while i < (2 ** 12):
            yield res
            i += 1

    def stimulate(self):
        return self._generator(random.random())
