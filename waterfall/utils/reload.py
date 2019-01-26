#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: reload.py
Author: dutyu
Date: 2018/08/13 22:06:02
Brief: reload
"""
import importlib
import sys

__all__ = ["mod_and_reload"]


def mod_and_reload(module_name, package, mod_func):
    spec = importlib.util.find_spec(module_name, package)
    source = spec.loader.get_source(module_name)
    new_source = mod_func(source)
    module = importlib.util.module_from_spec(spec)
    coded = compile(new_source, module.__spec__.origin, 'exec')
    exec(coded, module.__dict__)
    sys.modules[module_name] = module
    return module
