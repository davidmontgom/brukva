# -*- coding: utf-8 -*-
from functools import partial
import socket
import weakref
import logging
from time import sleep, time
from tornado import stack_context

from tornado.ioloop import IOLoop

from brukva.stream import BrukvaStream
from brukva.adisp import async, process
from brukva.exceptions import ConnectionError

log = logging.getLogger('brukva.pool')
log_blob = logging.getLogger('brukva.pool.blob')


class Connection(object):
    def __init__(self, host, port, idx,
                 on_connect, on_disconnect,
                 timeout=None, io_loop=None,
                 retries = None,
                 reconnect_timeout=None,
                 reconnect_retries=None):
        self.host = host
        self.port = port
        self.idx = idx

        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self.reconnect_timeout = reconnect_timeout
        self.reconnect_retries = reconnect_retries

        self.timeout = timeout
        self._io_loop = io_loop or IOLoop.instance()

        self._stream = None
        self.try_left = 2
        self._consume_buffer = ""
        self.is_initialized = False
        self.in_progress = False
        self.read_queue = []

    @process
    def connect(self, add_to_free=True):
        retries = self.reconnect_retries

        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
                sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))
                self._stream = BrukvaStream(sock, io_loop=self._io_loop)
                break
            except socket.error, e:
                log.error('Exception during connection!')
                log.error(e)

                log.info('Connect. Retry #%d' % retries)

                retries -= 1

                if retries <= 0:
                    raise ConnectionError("Exceeded retries to connect")

                if self._io_loop.running():
                    instance = self._io_loop.instance()
                    yield async(instance.add_timeout, time() + 1)()
                else:
                    sleep(self.reconnect_timeout)

        self._io_loop.add_callback(
            lambda: self.on_connect(weakref.proxy(self),
                                    add_to_free=add_to_free)
        )

    def disconnect(self):
        if self._stream:
            try:
                self._stream.close()
            except socket.error, e:
                pass
            self.is_initialized = False
            self._stream = None

    def write(self, data, callback, try_left=None, before_init=False):
        if try_left is None:
            try_left = self.try_left

        if not self._stream:
            try:
                self.connect(add_to_free=False)
            except ConnectionError as e:
                callback(e)

        if not self.is_initialized and not before_init:
            log_blob.debug('wait for connection initialization')
            self._io_loop.add_callback(
                lambda: self.write(data, callback, try_left))
            return

        if try_left > 0:
            try:
                log_blob.debug('try write %r to %s', data, self.idx)
                self._stream.write(data)

                log_blob.debug('data written to socket')
                callback(True)
                #self._io_loop.add_callback(lambda: callback(True))
            except IOError, e:
                log.error(e)
                self.disconnect()
                self.write(data, callback=callback, try_left=try_left - 1)
        else:
            callback(
                ConnectionError('Tried to write to non-existent connection'))

    def read(self, length, callback):
        try:
            if not self._stream:
                self.disconnect()
                callback(ConnectionError('Tried to read from non-existent connection'))

            log_blob.debug('read %s bytes from %s', length, self.idx)
            if self._consume_buffer:
                data = self._consume_buffer[:length]
                self._consume_buffer = self._consume_buffer[length:]
                callback(data)
            else:
                self._stream.read_bytes(length, callback)

        except IOError:
            self.on_disconnect(callback)

    def readline(self, callback):
        try:
            if not self._stream:
                self.disconnect()
                callback(ConnectionError('Tried to read from non-existent connection'))

            log_blob.debug('readline from %s', self.idx)
            if self._consume_buffer:
                log_blob.debug('consume buffer')
                splitted_buffer = self._consume_buffer.split('\r\n', 1)
                line = splitted_buffer[0] + '\r\n'
                self._consume_buffer = self._consume_buffer[len(line):]
                callback(line)
            else:
                log_blob.debug('no consume buffer')
                self._stream.read_until('\r\n', callback)
        except IOError:
            self.on_disconnect(callback)

    def readlines(self, num, callback):
        try:
            if not self._stream:
                self.disconnect()
                callback(ConnectionError(
                    'Tried to read from non-existent connection'))
            self._stream.read_until_times('\r\n', num, callback)
        except IOError:
            self.on_disconnect(callback)

    def read_multibulk_reply(self, num_answers, callback):
        try:
            if not self._stream:
                self.disconnect()
                callback(ConnectionError(
                    'Tried to read from non-existent connection'))

            if self._consume_buffer:
                callback(self._consume_buffer)
            else:
                self._stream.read_multibulk(num_answers, callback)
        except IOError:
            self.on_disconnect(callback)


class ConnectionPool(object):
    def __init__(self, connection_args, on_connect, on_disconnect, io_loop, pool_size=None):
        """
            connection_args:
            {
                'host' = 'localhost',
                port = 6379,
                timeout = None,

                selected_db = None,
                auth = None,
            }
        """
        self.pool_size = pool_size or 4
        self.connection_args = connection_args

        self._on_connect = on_connect
        self._on_disconnect = on_disconnect

        self.io_loop = io_loop

        self.connection_args['io_loop'] = io_loop
        self.connection_args['on_disconnect'] = on_disconnect

        self.connection_requests_queue = []

        self.is_connected = False


    def connect(self):
        """
            Create connection pool, connect all connection
                and perform AUTH, SELECT if needed.
        """
        self.connections = {}
        self.free_connections = set()
        connection_args = self.connection_args
        for idx in xrange(self.pool_size):
            @process
            def on_connect(connection, add_to_free=True):
                log_blob.debug(
                    'before invoke self._on_connect with connection %s',
                    connection.idx)
                yield async(self._on_connect)(connection)
                log_blob.debug(
                    'after invoke self._on_connect with connection %s',
                    connection.idx)
                if add_to_free:
                    self.free_connections.add(connection.idx)
                    log.info('connection %s added to pool',
                             connection.idx)
                self.io_loop.add_callback(self._give_out_pending_requests)

            connection_args['on_connect'] = on_connect
            connection_args['idx'] = idx
            connection = Connection(**connection_args)
            connection.connect()
            self.connections[idx] = connection
        self.is_connected = True

    def disconnect(self):
        for conn in self.connections.values():
            conn.disconnect()

    def _give_out_pending_requests(self):
        while self.connection_requests_queue and self.free_connections:
            log_blob.debug('late leasing connection')
            callback = self.connection_requests_queue.pop(0)
            self._lease_connection(callback)

    def return_connection(self, connection):
        self.free_connections.add(connection.idx)
        log_blob.debug('returned connection: %s', connection.idx)
        self._give_out_pending_requests()

    def _lease_connection(self, callback):
        idx = self.free_connections.pop()
        log_blob.debug('leasing connection %s', idx)
        connection = self.connections[idx]
        self.io_loop.add_callback(lambda: callback(connection))

    def request_connection(self, callback):
        if not self.free_connections:
            log_blob.debug('no free connections, waiting')
            self.connection_requests_queue.append(callback)
        else:
            self._lease_connection(callback)
