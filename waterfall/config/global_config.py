#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: global_config.py
Author: dutyu
Date: 2019/07/10 11:22:29
Brief: global_config
"""
import os

from waterfall.utils import fs
from waterfall.utils.validate import validate, validate2

__all__ = ["global_config"]


class GlobalConfig(object):
    DEFAULT_LOG_PATH = fs.path.dirname(os.environ['HOME'])
    LOG_PATH = DEFAULT_LOG_PATH

    @staticmethod
    def set_log_path(log_path):
        validate(log_path, str)
        validate2(log_path,
                  func=lambda path: os.path.isdir(path),
                  err_msg='please make sure that param log_path is an existed file path!')
        if log_path != GlobalConfig.DEFAULT_LOG_PATH \
                and GlobalConfig.LOG_PATH == GlobalConfig.DEFAULT_LOG_PATH:
            GlobalConfig.LOG_PATH = fs.path.dirname(log_path)


global_config = GlobalConfig
