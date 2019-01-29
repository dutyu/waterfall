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

__all__ = ["validate", ]


def validate(param, *_types, err_msg=None):
    type_set = set(_types)
    if None in _types:
        if param is None:
            return
        else:
            type_set.remove(None)
    if not isinstance(param, tuple(type_set)):
        if not err_msg:
            raise RuntimeError(
                'param\'s type should be in {:s} !'.format(
                    json.dumps(
                        list(map(lambda _type:
                                 str(type(_type)), _types)))))
        else:
            raise RuntimeError('validate err ! err_msg: {:s}'
                               .format(err_msg))


def validate2(*args, func=lambda *args: True, err_msg=''):
    validate(func, types.FunctionType)
    validate(err_msg, str)
    if not func(*args):
        raise RuntimeError(
            'validate err ! err_msg: {:s}'.format(err_msg))
