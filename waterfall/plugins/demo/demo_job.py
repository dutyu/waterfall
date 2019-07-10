#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: demo_job.py
Author: dutyu
Date: 2019/01/26 22:12:42
Brief: test_job
"""
import random
import time
import uuid

from waterfall.config.config import Config
from waterfall.job.job import Runnable, Job, FirstStep, Step


class TestRunner(Runnable):
    def _run(self, config, params, exit_flag):
        if exit_flag.value:
            return
        # print("params : " + str(params))
        print("config : " + str(config))
        time.sleep(0.01)
        # raise RuntimeError('test')
        print("run finish !")
        return (i for i in range(0, 100))


class TestRunner2(Runnable):
    def _run(self, config, params, exit_flag):
        if exit_flag.value:
            return
        print("params : " + str(params))
        print("config : " + str(config))
        j = 0
        res = random.random()
        while j < 100000:
            res = (2 * 21 ** 3 / 3.231 + 2 ** 4 / 3211.23231
                   - 342342 * 32 + random.random()) % random.random()
            j += 1
        print("run finish ! res: {:.2f}".format(res))
        return res


class TestJob(Job):
    @staticmethod
    def build():
        runner1 = TestRunner()
        runner2 = TestRunner2()
        first_step = FirstStep(runner1, 'thread', 10, 10)
        second_step = Step(runner2, 'thread', 8, 20)
        third_step = Step(runner1, 'thread', 100, 4000)
        first_step.set_next_step(second_step).set_next_step(third_step)
        return TestJob(uuid.uuid1(), 'job1',
                       Config().merge_from_dict(
                           {"test2": 2, "test3": 3}), first_step)

    @staticmethod
    def _generator(res):
        i = 0
        while i < (2 ** 12):
            yield res
            i += 1

    def stimulate(self):
        return self._generator(random.random())
