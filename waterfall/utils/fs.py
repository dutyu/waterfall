#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: fs.py
Author: dutyu
Date: 2019/01/26 16:59:03
Brief: fs
"""
import os
import platform

from waterfall.utils.singleton import singleton

__all__ = ["path"]


def _join(_path, *paths):
    if 'Windows' == platform.system():
        return os.path.join(_path, *paths) \
            .replace('\\', '/')
    return os.path.join(_path, *paths)


def _dirname(p):
    if 'Windows' == platform.system():
        return os.path.dirname(p) \
            .replace('\\', '/')
    return os.path.dirname(p)


@singleton
class Path(object):
    def __init__(self):
        object.__init__(self)

    @staticmethod
    def join(_path, *paths):
        return _join(_path, *paths)

    @staticmethod
    def dirname(p):
        return _dirname(p)


path = Path()
