#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: executor.py
Author: dutyu
Date: 2020/03/25 10:33:31
"""
import atexit
import pickle
import random
import uuid
import weakref
from concurrent.futures import Future
from multiprocessing.connection import wait
from queue import Full
from typing import List, Type, Dict

from kazoo.client import KazooClient

from waterfall._base import *
from waterfall._queue import RemoteSimpleQueue, RemoteQueue

_thread_queues = weakref.WeakKeyDictionary()
_zk_threads = weakref.WeakKeyDictionary()
_shutdown = False


def _python_exit():
    global _shutdown
    _shutdown = True
    items = list(_thread_queues.items())
    for t, q in items:
        q.put(None)
    for t, q in items:
        t.join()
    items = list(_zk_threads.items())
    for t, event in items:
        event.set()
        t.join()


_DEFAULT_TIMEOUT_SEC = 10


class Consumer(object):
    def __init__(self, zk_hosts: str, *,
                 port=CONSUMER_PORT):
        self._pending_work_items_lock = threading.Lock()
        self._fresh_providers_lock = threading.Lock()
        self._shutdown_lock = threading.Lock()
        self._queue_management_thread = None
        self._find_providers_thread = None
        self._timeout_check_thread = None
        self._result_queue_port = port
        self._pending_work_items = {}
        self.shutdown_thread = False
        self._remote_call_queues = {}
        self._result_queue = None
        self._zk_hosts = zk_hosts
        self._work_id = None
        self._broken = False
        self._providers = {}

    def get_providers(self, app_name: str) -> List[Type["ProviderItem"]]:
        with self._fresh_providers_lock:
            return self._providers.get(app_name)

    def _router(self, provider_items: List[Type["ProviderItem"]],
                args: List[Any], kwargs: Dict) -> str:
        pass

    def submit(self, app_name: str, service: str, args: List[Any] = None, kwargs: Dict = None, *,
               timeout: int = _DEFAULT_TIMEOUT_SEC) -> Future:

        # When the executor gets lost, the weakref callback will wake up
        # the queue management thread.
        def weakref_cb(_, q=self._result_queue):
            q.put(None)

        with self._shutdown_lock:
            self._work_id = uuid.uuid1()
            f = Future()

            if self._broken:
                raise BrokenQueueManager('The process of the queue terminated abruptly.')
            if self.shutdown_thread:
                raise RuntimeError('Cannot schedule new futures after shutdown.')

            # Start queue management thread
            if self._queue_management_thread is None:
                find_providers_latch = CountDownLatch(1)
                close_event = threading.Event()
                self._find_providers_thread = threading.Thread(
                    target=_find_providers,
                    args=(self._zk_hosts,
                          self._providers,
                          self._fresh_providers_lock,
                          self._pending_work_items,
                          self._pending_work_items_lock,
                          find_providers_latch,
                          close_event)
                )
                self._find_providers_thread.daemon = True
                self._find_providers_thread.start()
                _zk_threads[self._find_providers_thread] = close_event
                if not find_providers_latch.await(10):
                    raise TimeoutError(
                        'Connect to zk cluster timeout. zk hosts: {zk}'.format(
                            zk=self._zk_hosts)
                    )
                remote_queue = RemoteSimpleQueue(get_host_ip(),
                                                 self._result_queue_port)
                remote_queue.start()
                self._result_queue = remote_queue.queue
                self._queue_management_thread = threading.Thread(
                    target=_queue_management_worker,
                    args=(weakref.ref(self, weakref_cb),
                          self._pending_work_items,
                          self._result_queue,
                          remote_queue.process,
                          self._pending_work_items_lock))
                self._queue_management_thread.daemon = True
                self._queue_management_thread.start()

                self._timeout_check_thread = threading.Thread(
                    target=_timeout_check_worker,
                    args=(self._pending_work_items,
                          self._pending_work_items_lock)
                )
                self._timeout_check_thread.daemon = True
                self._timeout_check_thread.start()

                _thread_queues[self._queue_management_thread] = self._result_queue

            providers = self.get_providers(app_name)
            if not providers:
                # If executor can not find any provider, just reject the request.
                f.set_exception(
                    EmptyProvider('Reject request. Can not find any provider!')
                )
                return f

            # TODO 路由,等等实现,先随机路由
            provider_item = providers[random.randint(0, len(providers) - 1)]

            if not self._remote_call_queues.get(provider_item.id):
                remote_call_queue = RemoteQueue(provider_item.ip, provider_item.port)
                try:
                    remote_call_queue.connect()
                    self._remote_call_queues[provider_item.id] = remote_call_queue
                except:
                    f.set_exception(
                        RuntimeError(
                            'Connect to remote call queue failed, '
                            'provider_id: {provider_id}, traceback: {tb}'.format(
                                provider_id=provider_item.id,
                                tb=traceback.format_exc())
                        )
                    )
                    return f
                finally:
                    del remote_call_queue

            w = WorkItem(f, app_name, service, args, kwargs, provider_item.id, timeout)
            with self._pending_work_items_lock:
                self._pending_work_items[self._work_id] = w

            try:
                _add_call_item_to_queue(
                    w,
                    self._work_id,
                    self._result_queue_port,
                    self._remote_call_queues[provider_item.id])
            except Full:
                # Set exception if call_queue if Full now.
                w.future.set_exception(
                    ProviderTooBusy(
                        'Provider: {provider} is too busy now.'.format(
                            provider=w.provider_id
                        )
                    )
                )
                with self._pending_work_items_lock:
                    del self._pending_work_items[self._work_id]
            except:
                f.set_exception(
                    RuntimeError(
                        'Submit msg to provider failed, '
                        'provider: {provider}, traceback: {tb}'.format(
                            provider=w.provider_id,
                            tb=traceback.format_exc())
                    )
                )
                with self._pending_work_items_lock:
                    del self._pending_work_items[self._work_id]
            return f

    def shutdown(self, wait: bool = True) -> None:
        with self._shutdown_lock:
            self.shutdown_thread = True
        if self._queue_management_thread:
            # Wake up queue management thread
            self._result_queue.put(pickle.dumps(None))
            if wait:
                self._queue_management_thread.join()
        # To reduce the risk of opening too many files,
        # remove references to objects that use file descriptors.
        self._queue_management_thread = None
        self._result_queue = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)
        return False


def _timeout_check_worker(pending_work_items: Dict,
                          pending_work_items_lock: Type["threading.Lock"]) -> None:
    while not _shutdown:
        with pending_work_items_lock:
            work_items = set(pending_work_items.items())
        while work_items:
            k, item = work_items.pop()
            if item.future.cancelled():
                item.future.set_exception(
                    RuntimeError('Task has been canceled.')
                )
                with pending_work_items_lock:
                    try:
                        del pending_work_items[k]
                    except:
                        pass
            elif item.start_ts + item.timeout < time.time() \
                    and not item.future.done():
                item.future.set_exception(
                    RuntimeError(
                        'Task is timeout, '
                        'start time: {start_time}, '
                        'now: {now}, timeout: {timeout}s.'.format(
                            start_time=time.strftime(
                                '%Y-%m-%d %H:%M:%S',
                                time.localtime(item.start_ts)),
                            now=time.strftime('%Y-%m-%d %H:%M:%S'),
                            timeout=item.timeout)
                    )
                )
                with pending_work_items_lock:
                    try:
                        del pending_work_items[k]
                    except:
                        pass
            del item
        time.sleep(0.01)


def _queue_management_worker(executor_reference: Type["Consumer"],
                             pending_work_items: Dict,
                             result_queue: Type["multiprocessing.SimpleQueue"],
                             queue_process: Type["multiprocessing.Process"],
                             pending_work_items_lock: Type["threading.Lock"]) -> None:
    executor = None

    def shutting_down():
        return _shutdown or executor is None or executor.shutdown_thread

    def shutdown_manager():
        queue_process.terminate()
        queue_process.join()

    reader = result_queue._reader

    while True:
        ready = wait([reader] + [queue_process.sentinel])
        if reader in ready:
            result_item = reader.recv()
        else:
            # Mark the executor broken so that submits fail right now.
            executor = executor_reference()
            if executor is not None:
                executor._broken = True
                executor.shutdown_thread = True
                executor = None
            # All futures in flight must be marked failed
            with pending_work_items_lock:
                for work_id, work_item in pending_work_items.items():
                    work_item.future.set_exception(
                        BrokenQueueManager(
                            "The process of the queue manager was "
                            "terminated abruptly while the future was "
                            "running or pending."
                        ))
                    del work_item
                pending_work_items.clear()
            shutdown_manager()
            return
        if result_item is not None:
            with pending_work_items_lock:
                work_item = pending_work_items.pop(result_item.work_id, None)
            # work_item can be None if another process terminated (see above)
            if work_item is not None:
                if result_item.exception:
                    work_item.future.set_exception(result_item.exception)
                else:
                    work_item.future.set_result(result_item.result)
                # Delete references to object. See issue16284
                del work_item
        # Check whether we should start shutting down.
        executor = executor_reference()
        # No more work items can be added if:
        #   - The interpreter is shutting down OR
        #   - The executor that owns this worker has been collected OR
        #   - The executor that owns this worker has been shutdown.
        if shutting_down():
            # Since no new work items can be added, it is safe to shutdown
            # this thread if there are no pending work items.
            if not pending_work_items:
                shutdown_manager()
                return
        executor = None


def _add_call_item_to_queue(work_item: Type["WorkItem"],
                            work_id: str,
                            port: int,
                            call_queue: Type["multiprocessing.Queue"]) -> None:
    """Fills call_queue with _WorkItems from pending_work_items.

    This function never blocks.

    """
    if work_item.future.set_running_or_notify_cancel():
        call_queue.put_nowait(CallItem(work_id,
                                       port,
                                       work_item.service,
                                       work_item.args,
                                       work_item.kwargs))


def _find_providers(zk_hosts: str,
                    providers: Dict,
                    fresh_providers_lock: Type["threading.Lock"],
                    pending_work_items: Dict,
                    pending_work_items_lock: Type["threading.Lock"],
                    find_providers_latch: Type["CountDownLatch"],
                    close_event: Type["threading.Event"]) -> None:
    # Init zk client
    connection_retry = {'max_tries': -1, 'max_delay': 1}
    zk = KazooClient(hosts=zk_hosts,
                     connection_retry=connection_retry)
    zk.start()
    zk.ensure_path(ZK_PATH)
    child_nodes = zk.get_children(ZK_PATH)
    init_flag_dict = dict(map(
        lambda child_node: (child_node, True),
        child_nodes)
    )

    def _set_provider_listener(app_name: str) -> None:
        @zk.ChildrenWatch('/'.join(('', ZK_PATH, app_name)))
        def provider_listener(provider_nodes: List[str]) -> None:
            with fresh_providers_lock:
                providers[app_name] = (tuple(
                    map(lambda work_item:
                        ProviderItem(app_name, *work_item.split(':')),
                        provider_nodes)
                ))
            # Don't need to execute the below code if the listener be triggered first.
            if init_flag_dict[app_name]:
                init_flag_dict[app_name] = False
                return
            # Wait for providers to handle the pending work items.
            time.sleep(_DEFAULT_TIMEOUT_SEC + 1)
            # Use a filter to find the still remains pending items
            # and set a OfflineProvider exception.
            with pending_work_items_lock:
                remove_work_items = set(
                    filter(
                        lambda pair: pair[1].provider_id not in map(
                            lambda provider: provider.id, providers
                        ) and not pair[1].future.done(),
                        pending_work_items.items())
                )
            while remove_work_items:
                k, item = remove_work_items.pop()
                item.future.set_exception(
                    OfflineProvider(
                        'Remote provider is offline, '
                        'provider_id: {provider_id}.'.format(
                            provider_id=item.provider_id)
                    )
                )
                with pending_work_items_lock:
                    del pending_work_items[k]

    # Find all providers and add a watcher when nodes changes.
    for node in child_nodes:
        _set_provider_listener(node)

    find_providers_latch.count_down()
    close_event.wait()
    # Close zk client and release resources.
    zk.state_listeners.clear()
    try:
        zk.stop()
    except:
        traceback.print_exc()
    try:
        zk.close()
    except:
        traceback.print_exc()


atexit.register(_python_exit)
