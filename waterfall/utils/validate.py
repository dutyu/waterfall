#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: validate.py
Author: dutyu
Date: 2019/01/27 15:20:22
Brief: validate
"""
import json

__all__ = ["validate", ]


def validate(param, *types):
    if not isinstance(param, tuple(types)):
        raise RuntimeError(
            'param\'s type should be in {:s} !'.format(
                json.dumps(list(map(lambda _type: str(type(_type)), types)))))
