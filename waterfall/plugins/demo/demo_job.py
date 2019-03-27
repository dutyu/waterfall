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

from waterfall.config.config import Config
from waterfall.utils.singleton import job, step


@step('test_job')
def run(params, exit_flag):
    if exit_flag.value:
        return
    print("params : " + str(params))
    time.sleep(0.01)
    # raise RuntimeError('test')
    print("run finish !")
    return (i for i in range(0, 100))


@step('test_job')
def run(params, exit_flag):
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


@job('test_job', config=Config().merge_from_dict({"key1": "val1", "key2": "val2"}))
def stimulate():
    def _generator(res):
        i = 0
        while i < (2 ** 12):
            yield res
            i += 1

    return _generator(random.random())
