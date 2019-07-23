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

    PROGRESS_PERSISTENCE = False

    ENABLE_DEBUG = False

    ENABLE_MONITOR = True

    @staticmethod
    def set_log_path(log_path):
        """设置全局日志路径,默认路径为用户HOME路径"""
        validate(log_path, str)
        validate2(log_path,
                  func=lambda path: os.path.isdir(path),
                  err_msg='please make sure that param '
                          'log_path is an existed file path!')
        if log_path != GlobalConfig.DEFAULT_LOG_PATH \
                and GlobalConfig.LOG_PATH == GlobalConfig.DEFAULT_LOG_PATH:
            GlobalConfig.LOG_PATH = fs.path.dirname(log_path)

    @staticmethod
    def enable_debug_log():
        """开启debug日志,推荐调试时开启"""
        GlobalConfig.ENABLE_DEBUG = True

    @staticmethod
    def set_progress_persistence():
        """启用断点续传"""
        GlobalConfig.PROGRESS_PERSISTENCE = True

    @staticmethod
    def disable_monitor():
        """禁用monitor线程"""
        GlobalConfig.ENABLE_MONITOR = False


global_config = GlobalConfig
