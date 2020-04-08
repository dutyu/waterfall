#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: worker.py
Author: dutyu
Date: 2020/03/25 10:33:31
"""
import atexit
import multiprocessing
import weakref
from multiprocessing.connection import wait
from typing import Type
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.protocol.states import KazooState
from _base import *
from _queue import RemoteQueue, RemoteSimpleQueue

_processes_call_queue = weakref.WeakKeyDictionary()
_zk_threads = weakref.WeakKeyDictionary()


def _python_exit():
    items = list(_processes_call_queue.items())
    for q, processes in items:
        for _ in range(0, len(processes)):
            try:
                q.put_nowait(None)
            except:
                pass
        for _, p in processes.items():
            p.terminate()
            p.join()
    items = list(_zk_threads.items())
    for t, events in items:
        for e in events:
            e.set()
        t.join()


_DEFAULT_SEND_RETRY_TIMES = 1
_DEFAULT_MAX_QUEUE_SIZE = 1


class ProcessPoolProvider(object):
    def __init__(self, app_name: str, zk_hosts: str, *,
                 max_workers: int = 0,
                 max_q_size: int = -1,
                 port: int = PROVIDER_PORT):
        self._max_q_size = _DEFAULT_MAX_QUEUE_SIZE if max_q_size < 1 else max_q_size
        self._max_workers = os.cpu_count() if max_workers <= 0 else max_workers
        self._start_lock = threading.Lock()
        self._call_queue_process = None
        self._register_thread = None
        self._call_queue_port = port
        self._result_queue = None
        self._zk_hosts = zk_hosts
        self._app_name = app_name
        self._call_queue = None
        self._processes = {}
        self._state = 0
        self._zk = None

    def start(self) -> None:
        with self._start_lock:
            if self._state != 0:
                return
            self._state = 1
            # Start the queue process
            remote_queue = RemoteQueue(get_host_ip(),
                                       self._call_queue_port,
                                       self._max_q_size)
            remote_queue.start()
            self._call_queue = remote_queue.queue
            self._call_queue_process = remote_queue.process
            # self._call_queue._ignore_epipe = True
            # Start all sub processes
            self._adjust_process_count()
            # Use a CountDownLatch to wait for worker's registration
            register_latch = CountDownLatch(1)
            register_close_event = threading.Event()
            reconnected_event = threading.Event()
            # Start register thread to register itself to the cluster
            self._register_thread = threading.Thread(
                target=_register_worker,
                args=(self._zk_hosts,
                      self._app_name,
                      register_latch,
                      register_close_event,
                      reconnected_event,
                      self._call_queue_port)
            )
            self._register_thread.daemon = True
            self._register_thread.start()
            # To avoid unneeded create zk node action,
            # put the register_close_event at the former position
            _zk_threads[self._register_thread] = (register_close_event, reconnected_event)
            if not register_latch.await(10):
                raise TimeoutError(
                    'Connect to zk cluster timeout. zk hosts: {zk}'.format(
                        zk=self._zk_hosts)
                )
            # If any subprocess be killed, stop all sub processes and the parent process
            sentinels = [p.sentinel for p in self._processes.values()] + \
                        [self._call_queue_process.sentinel, ]
            assert sentinels
            wait(sentinels)
            self._shutdown_worker()

    def _adjust_process_count(self) -> None:
        for i in range(len(self._processes), self._max_workers):
            p = multiprocessing.Process(
                target=_process_worker,
                args=(self._call_queue,))
            p.start()
            self._processes[p.pid] = p
        _processes_call_queue[self._call_queue] = self._processes

    def _shutdown_worker(self) -> None:
        if self._state == 0:
            return
        # This is an upper bound
        for i in range(0, len(self._processes.values())):
            try:
                self._call_queue.put_nowait(None)
            except:
                pass
        try:
            self._call_queue.close()
        except:
            pass
        # Release the queue's resources as soon as possible.
        # If .join() is not called on the created processes then
        # some multiprocessing.Queue methods may deadlock on Mac OS X.
        for p in self._processes.values():
            p.terminate()
            p.join()
        p = self._call_queue_process
        p.terminate()
        p.join()


def _process_worker(call_queue: Type["multiprocessing.Queue"]) -> None:
    """Evaluates calls from call_queue and places the results in result_queue.

    This worker is run in a separate process.

    Args:
        call_queue: A multiprocessing.Queue of CallItems that will be read and
            evaluated by the worker.
    """

    remote_result_queues = {}
    retry_times = 0
    while True:
        retry_times %= _DEFAULT_SEND_RETRY_TIMES
        call_item = call_queue.get() if not retry_times else call_item

        if call_item is None:
            return

        q_id = call_item.consumer_id
        if not remote_result_queues.get(q_id):
            remote_queue = RemoteSimpleQueue(
                call_item.ip,
                call_item.port)
            try:
                remote_queue.connect()
                remote_result_queues[q_id] = remote_queue
            except:
                traceback.print_exc()
                del remote_queue
                retry_times += 1
                continue
        try:
            # TODO
            r = 1
        except BaseException as e:
            exc = ExceptionWithTraceback(e, e.__traceback__)
            try:
                remote_result_queues.get(q_id).put(
                    ResultItem(call_item.work_id, exception=exc)
                )
            except:
                traceback.print_exc()
                retry_times += 1
        else:
            try:
                remote_result_queues.get(q_id).put(
                    ResultItem(call_item.work_id, result=r)
                )
            except:
                traceback.print_exc()
                retry_times += 1


def _register_worker(zk_hosts: str,
                     app_name: str,
                     register_latch: Type["CountDownLatch"],
                     close_event: Type["threading.Event"],
                     reconnected_event: Type["threading.Event"],
                     port: int) -> None:
    def _register_path():
        return '/'.join(
            ('', ZK_PATH, app_name, ':'.join((IP, str(port))))
        )

    register_path = _register_path()
    # Init zk client and register itself
    connection_retry = {'max_tries': -1, 'max_delay': 1}
    zk = KazooClient(hosts=zk_hosts,
                     connection_retry=connection_retry)
    zk.start()
    try:
        zk.create(register_path, b'',
                  ephemeral=True, makepath=True)
    except NodeExistsError:
        pass

    def _listener(state):
        if state == KazooState.CONNECTED:
            reconnected_event.set()

    zk.add_listener(_listener)
    # Finish service registration, notice the main thread
    register_latch.count_down()

    while reconnected_event.wait():
        if close_event.is_set():
            break
        reconnected_event.clear()
        try:
            zk.create(register_path, b'',
                      ephemeral=True, makepath=True)
        except NodeExistsError:
            pass
    # Close zk client and release resources.
    zk.state_listeners.clear()
    try:
        zk.delete(register_path)
    except:
        pass
    try:
        zk.stop()
    except:
        traceback.print_exc()
    try:
        zk.close()
    except:
        traceback.print_exc()


atexit.register(_python_exit)
