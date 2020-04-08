#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: _util.py
Author: dutyu
Date: 2020/04/08 19:05:24
"""
import socket


def get_host_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip



