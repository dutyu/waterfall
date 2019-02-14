import random
import time
import uuid

from waterfall.config.config import Config
from waterfall.job.scheduler import JobScheduler
from waterfall.job.job import Job, FirstStep, Step, Runnable


class TestRunner(Runnable):
    def _run(self, params, exit_flag):
        if exit_flag.value:
            return
        print("params : " + str(params))
        time.sleep(0.01)
        # raise RuntimeError('test')
        print("run finish !")
        return (i for i in range(0, 100))


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


class TestJob(Job):
    @staticmethod
    def _generator(res):
        i = 0
        while i < (2 ** 12):
            yield res
            i += 1

    def stimulate(self):
        return self._generator(random.random())


if __name__ == "__main__":
    start_time = time.time()
    scheduler = JobScheduler(
        Config().merge_from_dict({"test": 1, "test2": 2}))
    runner1 = TestRunner()
    runner2 = TestRunner2()
    first_step = FirstStep(runner1, 'thread', 10, 10)
    second_step = Step(runner2, 'thread', 8, 20)
    third_step = Step(runner1, 'thread', 100, 4000)
    first_step.set_next_step(second_step).set_next_step(third_step)
    test_job = TestJob(uuid.uuid1(), 'job1',
                       Config().merge_from_dict(
                           {"test2": 2, "test3": 3}), first_step)
    scheduler.add_job(test_job)
    scheduler.set_ready().start()
    scheduler.close()
    print('cost time: {:.2f}'.format(time.time() - start_time))
