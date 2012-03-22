#!/usr/bin/env python
# -*- coding: utf-8 -*-

class RedisError(Exception):
    def __init__(self, message):
        Exception.__init__(self, "REDIS: %s" % message)


class ConnectionError(RedisError):
    pass


class RequestError(RedisError):
    def __init__(self, message, cmd_line=None):
        self.msg = message
        self.cmd_line = cmd_line
        RedisError.__init__(self, "%s %s" % (message, cmd_line))

    def __repr__(self):
        if self.cmd_line:
            return 'RequestError (on %s [%s, %s]): %s' % (
            self.cmd_line.cmd, self.cmd_line.args, self.cmd_line.kwargs, self.message)
        return 'RequestError: %s' % self.message

    __str__ = __repr__


class ResponseError(RedisError):
    def __init__(self, message, cmd_line=None):
        self.cmd_line = cmd_line
        self.msg = message
        RedisError.__init__(self, "%s %s" % (message, cmd_line))

    def __repr__(self):
        if self.cmd_line:
            return 'ResponseError (on %s [%s, %s]): %s' % (
            self.cmd_line.cmd, self.cmd_line.args, self.cmd_line.kwargs, self.msg)
        return 'ResponseError: %s' % self.message

    __str__ = __repr__


class InternalRedisError(ResponseError):
    pass

class InvalidResponse(RedisError):
    pass
