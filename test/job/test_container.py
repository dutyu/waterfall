import random
import time

from multiprocessing import Manager

from waterfall.config.config import Config
from waterfall.job.job import JobsContainer, Job, FirstStep, Step
from waterfall.logger import Logger


class TestRunner(Step.Runnable):
    def run(self, params, res_queue, monitor_queue, err_flag):
        try:
            print("params : " + str(params))
            time.sleep(0.1)
            print("run finish !")
            if res_queue:
                for i in range(10):
                    res_queue.put(random.random())
                    monitor_queue.put(random.random())
        except Exception as e:
            Logger().error_logger.exception(e)
            err_flag.value = True


class TestJob(Job):
    def stimulate(self):
        queue = Manager().Queue()
        for i in range(10):
            queue.put(random.random())
        return queue


if __name__ == "__main__":
    container = JobsContainer(Config().merge_from_dict({"test": 1, "test2": 2}))
    runner1 = TestRunner()
    first_step = FirstStep(10, 'process', runner1)
    second_step = Step(2, 'thread', runner1)
    first_step.set_next_step(second_step)
    test_job = TestJob(Config().merge_from_dict({"test2": 2, "test3": 3}), first_step)
    container.add_job(test_job)
    container.set_ready().start()
