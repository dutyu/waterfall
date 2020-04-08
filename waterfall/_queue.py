#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: _queue.py
Author: dutyu
Date: 2020/04/07 10:48:15
"""
import multiprocessing
import pickle
import socket
import socketserver
import time
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import SimpleQueue
from queue import Full
from typing import Any, Union


class State(object):
    __slots__ = ['value']
    INITIAL = 0
    #
    CONNECTED = 1
    #
    STARTED = 2


class RemoteSimpleQueue(object):
    def __init__(self, ip: str, port: int, max_size: int=-1) -> None:
        self._ip = ip
        self._port = port
        self._max_size = max_size
        self.queue = None
        self.process = None
        self._state = State()
        self._state.value = State.INITIAL

    def _start(self, queue: multiprocessing.SimpleQueue) -> None:
        # Spawn a server process
        assert self._state.value == State.INITIAL
        self.queue = queue
        self.process = Process(
            target=_queue_server_worker,
            args=(self._ip, self._port, self.queue)
        )
        self.process.start()
        time.sleep(1)
        self._state.value = State.STARTED

    def start(self) -> None:
        self._start(SimpleQueue())

    def connect(self) -> None:
        # Connect to the remote server
        assert self._state.value == State.INITIAL
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1)
        # Connect to server and send data
        sock.sendto(b'\n', (self._ip, self._port))
        self._state.value = State.CONNECTED

    def put(self, obj: Any) -> None:
        assert self._state.value != State.INITIAL
        data = pickle.dumps(obj)
        if self._state.value == State.CONNECTED:
            # Connect to server and send data
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(1)
            sock.sendto(data + b'\n', (self._ip, self._port))
        else:
            self.queue.put(data)

    def get(self) -> Any:
        assert self._state.value == State.STARTED
        data = self.queue.get()
        return pickle.loads(data)


class RemoteQueue(RemoteSimpleQueue):
    def start(self) -> None:
        self._start(Queue() if self._max_size < 1 else Queue(self._max_size))

    def put_nowait(self, obj: Any) -> None:
        assert self._state.value != State.INITIAL
        data = pickle.dumps(obj)
        if self._state.value == State.CONNECTED:
            # Send data to server
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(1)
            sock.sendto(data + b'\n', (self._ip, self._port))
            res = sock.recv(1024)
            if res.startswith(b'#FULL#'):
                raise Full()
        else:
            self.queue.put_nowait(data)

    def get_nowait(self) -> Any:
        assert self._state.value == State.STARTED
        try:
            data = self.queue.get_nowait()
        except:
            raise
        else:
            return pickle.loads(data)

    def close(self):
        assert self._state.value == State.STARTED
        self.queue.close()


def _queue_server_worker(ip: str, port: int,
                         queue: Union["Queue", "SimpleQueue"]) -> None:
    class QueueHandler(socketserver.BaseRequestHandler):
        def handle(self):
            data, sock = self.request
            data = data.strip()
            if not data:
                return
            data = pickle.loads(data)
            if not hasattr(queue, 'put_nowait'):
                queue.put(data)
            else:
                try:
                    queue.put_nowait(data)
                except Full:
                    sock.sendto(bytes('#FULL#', 'utf8') + b'\n',
                                self.client_address)
                else:
                    sock.sendto(b'\n', self.client_address)

    with socketserver.UDPServer((ip, port), QueueHandler) as server:
        server.serve_forever()

