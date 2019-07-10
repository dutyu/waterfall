import time

from waterfall.config.config import Config
from waterfall.job.scheduler import JobScheduler
from waterfall.plugins.demo.demo_job import TestJob

if __name__ == "__main__":
    start_time = time.time()
    scheduler = JobScheduler(
        Config().merge_from_dict({"test": 1, "test2": 2}))
    scheduler.add_job(TestJob.build())
    scheduler.set_ready().start()
    scheduler.close()
    print('cost time: {:.2f}'.format(time.time() - start_time))
