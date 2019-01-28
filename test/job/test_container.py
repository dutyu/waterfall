import random
import threading
import time

from multiprocessing import Manager

from waterfall.config.config import Config
from waterfall.job.job import JobsContainer, Job, FirstStep, Step
from waterfall.logger import Logger


class TestRunner(Step.Runnable):
    def _run(self, params, exit_flag):
        try:
            print("params : " + str(params))
            time.sleep(1)
            print("run finish !")
            return (i for i in range(0, 10))
        except Exception as e:
            Logger().error_logger.exception(e)


class TestJob(Job):
    def stimulate(self):
        queue = Manager().Queue()
        for i in range(10):
            queue.put(random.random())
        return queue


if __name__ == "__main__":
    container = JobsContainer(Config().merge_from_dict({"test": 1, "test2": 2}))
    runner1 = TestRunner()
    first_step = FirstStep(runner1, 'process', 2, 10)
    second_step = Step(runner1, 'thread', 5, 5)
    first_step.set_next_step(second_step)
    test_job = TestJob('job1', Config().merge_from_dict({"test2": 2, "test3": 3}), first_step)
    container.add_job(test_job)
    container.set_ready().start()
