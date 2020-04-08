import faulthandler
import platform
import signal

from waterfall.provider import ProcessPoolProvider

if __name__ == '__main__':

    if platform.system() == 'Linux':
        faulthandler.register(signal.SIGUSR1)
        faulthandler.enable()

    worker = ProcessPoolProvider(
        'test-app',
        '127.0.0.1:2181',
        max_workers=10
    )

    worker.start()
