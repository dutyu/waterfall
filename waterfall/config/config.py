#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: config.py
Author: dutyu
Date: 2019/01/26 20:58:32
Brief: config
"""
import copy
import json

from waterfall.utils import http
from waterfall.utils.validate import validate


class Config(object):
    __slots__ = ['_dict', ]

    def __init__(self):
        self._dict = {}

    def set(self, k, v, overwrite=True):
        validate(k, str)
        return self._set(self._dict, k, v, overwrite)

    def _set(self, d, k, v, overwrite):
        validate(d, dict)
        validate(k, str)
        k_list = k.strip().split('.')
        if len(k_list) == 1:
            if overwrite:
                d[k] = v
            else:
                if k not in d:
                    d[k] = v
            return self
        temp_k = k_list[0].strip()
        temp_v = d.get(temp_k)
        if not temp_v:
            d[temp_k] = {}
        return self._set(d[temp_k], '.'.join(k_list[1:]), v, overwrite)

    def merge_from_dict(self, _dict, overwrite=True):
        validate(_dict, dict)
        if overwrite:
            self._dict.update(_dict)
        else:
            temp_dict = copy.deepcopy(_dict)
            temp_dict.update(self._dict)
            self._dict = temp_dict
        return self

    def merge_from_json(self, json_str, overwrite=True):
        _dict = json.loads(json_str)
        return self.merge_from_dict(_dict, overwrite)

    def merge_from_http(self, url, overwrite=True):
        return self.merge_from_json(http.get(url), overwrite)

    def get_val(self, k, default_v=''):
        return self._get_val(self._dict, k, default_v)

    def get_int(self, k, default_v=0):
        return int(self.get_val(k, default_v))

    def get_float(self, k, default_v=0):
        return float(self.get_val(k, default_v))

    def get_str(self, k, default_v=''):
        return str(self.get_val(k, default_v))

    def _get_val(self, d, k, default_v):
        validate(d, dict)
        validate(k, str)
        k_list = k.strip().split('.')
        if len(k_list) == 1:
            return default_v if d.get(k.strip()) is None \
                else d.get(k.strip())
        temp_k = k_list[0].strip()
        return self._get_val(d.get(temp_k),
                             '.'.join(k_list[1:]), default_v)

    def __str__(self):
        return json.dumps(self._dict)
