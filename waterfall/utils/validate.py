#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: validate.py
Author: dutyu
Date: 2019/01/27 15:20:22
Brief: validate
"""
import json
import types

import itertools
from typing import Iterable

__all__ = ["validate", ]

_SENTINEL = object()


def validate(param, *_types, err_msg=''):
    type_set = set(_types)
    if None in _types:
        if param is None:
            return
        else:
            type_set.remove(None)
    _err_msg = 'validate err ! err_msg: {:s}' \
        .format(err_msg) if err_msg.strip() != '' else \
        'param\'s type should be in {:s} !'.format(
            json.dumps(
                list(map(lambda _type:
                         str(type(_type)), _types))))
    if not isinstance(param, tuple(type_set)):
        raise ValueError(_err_msg)


def validate2(*args, func=lambda *args, **kwargs: True, err_msg=''):
    validate(func, types.FunctionType)
    validate(err_msg, str)
    if not func(*args):
        raise ValueError(
            'validate err ! err_msg: {:s}'.format(err_msg))


def at_most(size, iterable, err_msg=''):
    validate(size, int)
    validate(iterable, Iterable)
    validate(err_msg, str)
    _err_msg = 'validate err ! err_msg: {:s}' \
        .format(err_msg) if err_msg.strip() != '' else \
        'validate err, too large iterable obj !'
    if size < 0:
        raise ValueError('size must be positive (or zero) !')
    elif hasattr(iterable, '__len__'):
        if len(iterable) > size:
            raise ValueError(_err_msg)
    else:
        g = itertools.islice(iterable, size, None)
        if not next(g, _SENTINEL) is _SENTINEL:
            raise ValueError(_err_msg)
