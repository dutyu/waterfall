#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: encrypt.py
Author: dutyu
Date: 2018/04/02 16:28:34
Brief: encrypt
"""
import base64
from Crypto.Cipher import AES

__all__ = ["encrypt", "decrypt"]


def _pad_func(s):
    return s + (16 - len(s) % 16) * '\0'


def encrypt(key, text):
    crypto = AES.new(key, AES.MODE_CBC, b'0000000000000000')
    ciphered = crypto.encrypt(_pad_func(text))
    return base64.b64encode(ciphered)


def decrypt(key, text):
    crypto = AES.new(key, AES.MODE_CBC, b'0000000000000000')
    plain_text = crypto.decrypt(base64.b64decode(text))
    return plain_text.rstrip(b'\x00').decode("utf-8")
