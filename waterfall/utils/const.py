#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: const.py
Author: dutyu
Date: 2019/16/32 16:59:03
Brief: const
"""
from waterfall.utils import fs

__all__ = ["const"]


class Const(object):
    ROOT_PATH = fs.path.dirname(fs.path.dirname(__file__))


const = Const
