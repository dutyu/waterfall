#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

from examples.simple_demo.demo_job import TestJob
from waterfall.config.config import Config
from waterfall.config.global_config import GlobalConfig
from waterfall.job.scheduler import JobScheduler

if __name__ == "__main__":
    start_time = time.time()
    # 设置日志路径
    GlobalConfig.set_log_path("./logs/")

    scheduler = JobScheduler(
        Config().merge_from_dict({"test": 1, "test2": 2}))
    scheduler.add_job(TestJob.build())
    scheduler.set_ready().start()
    scheduler.close()

    print('cost time: {:.2f}'.format(time.time() - start_time))
