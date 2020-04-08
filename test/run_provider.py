import faulthandler
import platform
import signal

from waterfall.registration import RegistrationCenter
from waterfall.provider import ProcessPoolProvider

if __name__ == '__main__':

    if platform.system() == 'Linux':
        faulthandler.register(signal.SIGUSR1)
        faulthandler.enable()

    def _fn(a, b):
        return a + b

    RegistrationCenter.register_service('test_service', _fn)

    worker = ProcessPoolProvider(
        'test-app2',
        '127.0.0.1:2181',
        max_workers=1
    )

    worker.start(10)
