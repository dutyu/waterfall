import faulthandler
import platform
import random
import signal
import time
import traceback

from waterfall.consumer import Consumer

if __name__ == '__main__':

    if platform.system() == 'Linux':
        faulthandler.register(signal.SIGUSR1)
        faulthandler.enable()

    executor = Consumer('127.0.0.1:2181')
    futures = []
    start_ts = time.time()
    for _ in range(0, 2000):
        for __ in range(0, 10000):
            f = executor.submit(
                'test-app2',
                'test_service',
                args=(random.randint(0, 100), random.randint(0, 100)),
                timeout=1
            )
            futures.append(f)
        for f in futures:
            try:
                print(f.result())
            except:
                exception = traceback.print_exc()
        futures.clear()
    print('it cost: {cost} S.'.format(cost=time.time() - start_ts))
