#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: http.py
Author: dutyu
Date: 2018/12/19 15:46:12
Brief: http
"""
import re
import requests

from waterfall.logger import Logger

__all__ = ["get", "post"]

_DEFAULT_CHUNK_SIZE = 8


def _validate_url(url):
    if not re.match(r"^https?:/{2}\w.+$", url):
        err_msg = 'Wrong http url format: {:s} !'.format(url)
        Logger().error_logger.error(err_msg)
        raise RuntimeError(err_msg)


def get(url, res_type='text', *, headers={}, params={}):
    _validate_url(url)
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    if 'text' == res_type:
        return r.text
    if 'json' == res_type:
        return r.json()
    return r.raw


def post(url, res_type='text', *, data="", files={}):
    _validate_url(url)
    r = requests.post(url, data=data, files=files, timeout=10)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    if 'text' == res_type:
        return r.text
    if 'json' == res_type:
        return r.json()
