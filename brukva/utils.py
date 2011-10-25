# -*- coding: utf-8 -*-
from datetime import datetime
from itertools import izip

import traceback
from collections import Iterable
import logging

log = logging.getLogger('brukva.utils')

class ExecutionContext(object):
    def __init__(self, callbacks, error_wrapper=None):
        self.callbacks = callbacks
        self.error_wrapper = error_wrapper
        self.is_active = True

    def _call_callbacks(self, callbacks, value):
        if callbacks:
            if isinstance(callbacks, Iterable):
                for cb in callbacks:
                    cb(value)
            else:
                callbacks(value)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        if type_ is None:
            return True

        if self.error_wrapper:
            value = self.error_wrapper(value)
        else:
            value = value or Exception(
                'Strange exception with None value type: %s; tb: %s' %
                (type_, '\n'.join(traceback.format_tb(tb))
            ))

        if self.is_active:
            log.error(value, exc_info=(type_, value, tb))
            self.ret_call(value)
            return True
        else:
            return False

    def disable(self):
        self.is_active = False

    def enable(self):
        self.is_active = True

    def ret_call(self, value):
        self.is_active = False
        self._call_callbacks(self.callbacks, value)
        self.is_active = True

    def safe_call(self, callbacks, value):
        self.is_active = False
        self._call_callbacks(callbacks, value)
        self.is_active = True


def execution_context(callbacks, error_wrapper=None):
    """
    Syntax sugar.
    If some error occurred inside with block,
    it will be suppressed and forwarded to callbacks.

    Use contex.ret_call(value) method to call callbacks.

    @type callbacks: callable or iterator over callables
    @rtype: context
    """
    return ExecutionContext(callbacks, error_wrapper)


def string_keys_to_dict(key_string, callback):
    return dict([(key, callback) for key in key_string.split()])


def encode(value):
    if isinstance(value, str):
        return value
    elif isinstance(value, unicode):
        return value.encode('utf-8')
        # pray and hope
    return str(value)


def format(*tokens):
    cmds = []
    for t in tokens:
        e_t = encode(t)
        cmds.append('$%s\r\n%s\r\n' % (len(e_t), e_t))
    return '*%s\r\n%s' % (len(tokens), ''.join(cmds))


def format_pipeline_request(command_stack):
    """
        @command_stack: [CmdLine(), ...]
        @return: str
        Return serialized request to redis for pipeline
    """
    return ''.join(format(c.cmd, *c.args, **c.kwargs) for c in command_stack)

def reply_to_bool(r, *args, **kwargs):
    return bool(r)


def make_reply_assert_msg(msg):
    def reply_assert_msg(r, *args, **kwargs):
        return r == msg

    return reply_assert_msg


def reply_set(r, *args, **kwargs):
    return set(r)


def reply_dict_from_pairs(r, *args, **kwargs):
    return dict(izip(r[::2], r[1::2]))


def reply_str(r, *args, **kwargs):
    return r or ''


def reply_int(r, *args, **kwargs):
    return int(r) if r is not None else None


def reply_float(r, *args, **kwargs):
    return float(r) if r is not None else None


def reply_datetime(r, *args, **kwargs):
    return datetime.fromtimestamp(int(r))


def reply_zset(r, *args, **kwargs):
    if (not r ) or (not 'WITHSCORES' in args):
        return r
    return zip(r[::2], map(float, r[1::2]))


def reply_hmget(r, key, *fields, **kwargs):
    return dict(zip(fields, r))


def reply_info(response):
    info = {}

    def get_value(value):
        if ',' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict

    for line in response.splitlines():
        key, value = line.split(':')
        try:
            info[key] = int(value)
        except ValueError:
            info[key] = get_value(value)
    return info


def reply_ttl(r, *args, **kwargs):
    return r != -1 and r or None
