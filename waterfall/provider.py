#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: worker.py
Author: dutyu
Date: 2020/03/25 10:33:31
"""
import atexit
import multiprocessing
import os
import threading
import traceback
import weakref
from multiprocessing.connection import wait

from typing import Dict, Callable

from waterfall.registration import RegistrationCenter
from waterfall import _util
from waterfall import _base
from waterfall import _queue

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


_DEFAULT_MAX_QUEUE_SIZE = 1000


class ProcessPoolProvider(object):
    def __init__(self, app_name: str, zk_hosts: str, *,
                 max_workers: int = 0,
                 max_q_size: int = -1,
                 port: int = _base.PROVIDER_PORT):
        self._max_q_size = _DEFAULT_MAX_QUEUE_SIZE if max_q_size < 1 else max_q_size
        self._max_workers = os.cpu_count() if max_workers <= 0 else max_workers
        self._start_lock = threading.Lock()
        self._registration_center = None
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

    def start(self, init_weight: int = 100) -> None:
        init_weight = 100 if init_weight > 100 else init_weight
        with self._start_lock:
            if self._state != 0:
                return
            self._state = 1
            # Start the queue process
            remote_queue = _queue.RemoteQueue(_util.get_host_ip(),
                                              self._call_queue_port,
                                              self._max_q_size)
            remote_queue.start()
            self._call_queue = remote_queue.queue
            self._call_queue._ignore_epipe = True
            self._call_queue_process = remote_queue.process
            # Use a CountDownLatch to wait for worker's registration
            register_latch = _base.CountDownLatch(1)
            register_close_event = threading.Event()
            reconnected_event = threading.Event()
            # Start register thread to register itself to the cluster
            self._registration_center = RegistrationCenter(
                self._zk_hosts,
                self._call_queue_port,
                self._app_name,
                register_close_event,
                register_latch)
            self._register_thread = self._registration_center.start_provider_register_thread(
                reconnected_event,
                init_weight)
            # To avoid unneeded create zk node action,
            # put the register_close_event at the former position
            _zk_threads[self._register_thread] = (register_close_event, reconnected_event)
            if not register_latch.await(10):
                raise TimeoutError(
                    'Connect to zk cluster timeout. zk hosts: {zk}'.format(
                        zk=self._zk_hosts)
                )
            # Start all sub processes
            self._adjust_process_count()
            # If any subprocess be killed, stop all sub processes and the parent process
            sentinels = [p.sentinel for p in self._processes.values()] + \
                        [self._call_queue_process.sentinel, ]
            assert sentinels
            self._hello_world()
            wait(sentinels)
            self._shutdown_worker()

    def _hello_world(self):
        print('#' * 50)
        print('#' * 11 + '   WATERFALL HAS STARTED!   ' + '#' * 11)
        print('#' * 11 + '        VERSION 0.2.0       ' + '#' * 11)
        print('#' * 11 + '        AUTHOR: dutyu       ' + '#' * 11)
        print('#' * 11 + ' MAIL:dut.xiangyu@gmail.com ' + '#' * 11)
        space_cnt = 23 - len(self._app_name)
        left_space_cnt = int(space_cnt / 2)
        print('#' * 11 + ' ' * left_space_cnt + 'APP: {app_name}'.format(
            app_name=self._app_name) + ' ' * (space_cnt - left_space_cnt) + '#' * 11)
        print('#' * 50)

    def _adjust_process_count(self) -> None:
        for i in range(len(self._processes), self._max_workers):
            p = multiprocessing.Process(
                target=_process_worker,
                args=(self._call_queue,
                      self._registration_center.get_services()))
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


def _process_worker(call_queue: multiprocessing.Queue,
                    services: Dict[str, Callable]) -> None:
    """Evaluates calls from call_queue and places the results in result_queue.

    This worker is run in a separate process.

    Args:
        call_queue: A multiprocessing.Queue of CallItems that will be read and
            evaluated by the worker.
    """

    remote_result_queues = {}
    retry_times = 0
    while True:
        retry_times %= _base.DEFAULT_SEND_RETRY_TIMES
        call_item = call_queue.get() if not retry_times else call_item

        if call_item is None:
            return

        q_id = call_item.consumer_id
        if not remote_result_queues.get(q_id):
            remote_queue = _queue.RemoteSimpleQueue(
                call_item.ip,
                call_item.port)
            try:
                remote_queue.connect()
                remote_result_queues[q_id] = remote_queue
            except:
                traceback.print_exc()
                retry_times += 1
                continue
        try:
            r = services[call_item.service](*call_item.args, **call_item.kwargs)
        except BaseException as e:
            exc = _base.ExceptionWithTraceback(e, e.__traceback__)
            try:
                remote_result_queues.get(q_id).put(
                    _base.ResultItem(call_item.work_id, exception=exc)
                )
            except:
                traceback.print_exc()
                retry_times += 1
        else:
            try:
                remote_result_queues.get(q_id).put(
                    _base.ResultItem(call_item.work_id, result=r)
                )
            except:
                traceback.print_exc()
                retry_times += 1


atexit.register(_python_exit)
