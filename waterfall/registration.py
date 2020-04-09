#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: registration.py
Author: dutyu
Date: 2020/04/08 14:37:52
"""
import os
import pickle
import threading
import traceback
from types import MappingProxyType
from typing import Callable, List, Dict
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.protocol.states import KazooState

from waterfall import _util
from waterfall import _base


@_util.singleton
class RegistrationCenter(object):

    _SERVICES_ = dict()

    def __init__(self,
                 zk_hosts: str,
                 port: int,
                 app_name: str,
                 close_event: threading.Event,
                 init_latch: _base.CountDownLatch
                 ) -> None:
        self._close_event = close_event
        self._init_latch = init_latch
        self._app_name = app_name
        self._zk_hosts = zk_hosts
        # When python exit hook func be triggered,
        # provider's process will delete the zk node,
        # so we need to record the process when create the zk node,
        # if the process isn't the owner, do nothing.
        self._create_pid = None
        self._port = port

    def _register_path(self) -> str:
        return '/'.join(
            ('', _base.ZK_PATH, self._app_name,
             ':'.join((_base.IP, str(self._port))))
        )

    @classmethod
    def register_service(cls, service_name: str,
                         fn: Callable) -> None:
        cls._SERVICES_[service_name] = fn

    def get_services(self) -> MappingProxyType:
        return MappingProxyType(self._SERVICES_)

    def _find_provider(self,
                       pending_work_items_lock: threading.Lock,
                       providers: Dict[str, _base.ProviderItem],
                       providers_lock: threading.Lock,
                       pending_work_items: Dict) -> None:
        # Init zk client.
        connection_retry = {'max_tries': -1, 'max_delay': 1}
        zk = KazooClient(hosts=self._zk_hosts,
                         connection_retry=connection_retry)
        zk.start()
        zk.ensure_path(_base.ZK_PATH)
        child_nodes = zk.get_children(_base.ZK_PATH)

        def _set_provider_listener(app_name: str) -> None:
            app_zk_path = '/'.join((_base.ZK_PATH, app_name))
            providers[app_name] = {}

            def _set_provider_data_listener(provider_node):
                @zk.DataWatch('/'.join((app_zk_path, provider_node)))
                def data_listener(data, no_use):
                    if not data:
                        return
                    with providers_lock:
                        providers[app_name][provider_node] = _base.ProviderItem(
                            app_name,
                            *provider_node.split(':'),
                            pickle.loads(data))

            @zk.ChildrenWatch(app_zk_path)
            def provider_listener(provider_nodes: List[str]) -> None:
                offline_flag = False
                for provider_node in (set(providers[app_name].keys()) - set(provider_nodes)):
                    offline_flag = True
                    with providers_lock:
                        del providers[app_name][provider_node]

                # Add a data watcher to the provider node which we haven't listened.
                for provider_node in (set(provider_nodes) - set(providers[app_name].keys())):
                    _set_provider_data_listener(provider_node)

                # Don't need to execute the below code if there is no offline node.
                if not offline_flag:
                    return

                # Wait for providers to handle the pending work items.
                time.sleep(_base.DEFAULT_TIMEOUT_SEC + 1)
                # Use a filter to find still remains pending items
                # and set a OfflineProvider exception.
                remove_work_items = set(
                    filter(
                        lambda pair: pair[1].provider_id not in provider_nodes
                        and not pair[1].future.done(),
                        tuple(pending_work_items.items())))
                while remove_work_items:
                    k, item = remove_work_items.pop()
                    item.future.set_exception(
                        _base.OfflineProvider(
                            'Remote provider is offline, '
                            'provider_id: {provider_id}.'.format(
                                provider_id=item.provider_id)
                        )
                    )
                    with pending_work_items_lock:
                        try:
                            del pending_work_items[k]
                        except:
                            pass

        # Find all providers and add a watcher when nodes changes.
        for node in child_nodes:
            _set_provider_listener(node)
        # We have finished the init stage,
        # so notice the main thread to continue.
        self._init_latch.count_down()
        # Wait for close event.
        self._close_event.wait()
        self._close(zk, False)

    def _register_provider(self, reconnected_event: threading.Event,
                           init_weight: int) -> None:
        # Init zk client and register itself
        connection_retry = {'max_tries': -1, 'max_delay': 1}
        zk = KazooClient(hosts=self._zk_hosts,
                         connection_retry=connection_retry)
        zk.start()

        services = dict(map(lambda service:
                            (service, _base.ServiceItem(service, init_weight)),
                            self._SERVICES_))
        meta_info = _base.ProviderMetaInfo(init_weight, services)

        try:
            zk.create(self._register_path(),
                      pickle.dumps(meta_info),
                      ephemeral=True, makepath=True)
        except NodeExistsError:
            pass

        def _listener(state):
            if state == KazooState.CONNECTED:
                reconnected_event.set()

        zk.add_listener(_listener)
        # Finish service registration, notice the main thread
        self._init_latch.count_down()
        # When reconnected event occurs, recreate the worker node.
        while reconnected_event.wait():
            if self._close_event.is_set():
                break
            reconnected_event.clear()
            try:
                zk.create(self._register_path(),
                          pickle.dumps(meta_info),
                          ephemeral=True, makepath=True)
            except NodeExistsError:
                pass
        self._close(zk, True)

    def _close(self, zk: KazooClient, is_provider: bool) -> None:
        # Close zk client and release resources.
        zk.state_listeners.clear()
        if is_provider and os.getpid() == self._create_pid:
            for service in self._SERVICES_:
                try:
                    zk.delete('/'.join((self._register_path(), service)))
                except NodeExistsError:
                    pass
        try:
            zk.stop()
        except:
            traceback.print_exc()
        try:
            zk.close()
        except:
            traceback.print_exc()

    def start_provider_register_thread(self,
                                       reconnected_event: threading.Event,
                                       init_weight: 100
                                       ) -> threading.Thread:
        t = threading.Thread(
            target=self._register_provider,
            args=(reconnected_event,
                  init_weight)
        )
        t.daemon = True
        t.start()
        return t

    def start_find_worker_thread(self,
                                 pending_work_items_lock: threading.Lock,
                                 providers: Dict[str, _base.ProviderItem],
                                 providers_lock: threading.Lock,
                                 pending_work_items: Dict[str, _base.WorkItem]
                                 ) -> threading.Thread:
        t = threading.Thread(
            target=self._find_provider,
            args=(pending_work_items_lock,
                  providers,
                  providers_lock,
                  pending_work_items)
        )
        t.daemon = True
        t.start()
        return t
