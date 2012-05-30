# -*- coding: utf-8 -*-
from collections import  defaultdict
import weakref
from redis.client import  dict_merge
from tornado.ioloop import IOLoop

from adisp import async, process

from brukva.utils import *
from brukva.pool import ConnectionPool
from brukva.exceptions import  ConnectionError, ResponseError, RedisError, InternalRedisError

log = logging.getLogger('brukva.client')
log_blob = logging.getLogger('brukva.client.blob')

class CmdLine(object):
    def __init__(self, cmd, *args, **kwargs):
        self.cmd = cmd
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return self.cmd + '(' + str(self.args) + ',' + str(self.kwargs) + ')'


class _AsyncWrapper(object):
    def __init__(self, obj):
        self.obj = weakref.proxy(obj)
        self.memoized = {}

    def __getattr__(self, item):
        if item not in self.memoized:
            if getattr(self.obj, 'yield_mode'):
                self.memoized[item] = getattr(self.obj, item)
            else:
                self.memoized[item] = async(getattr(self.obj, item), cbname='callbacks')
        return self.memoized[item]


class Client(object):
    def __init__(self, host='localhost', port=6379, password=None,
                 db=None, io_loop=None, yield_mode=False, pool_size=None,
                 retries = None, reconnect_timeout=None, reconnect_retries=None):
        self.yield_mode = yield_mode
        self.memoized = {}
        self._io_loop = io_loop or IOLoop.instance()
        self.host = host
        self.port = port

        self.connection_pool = ConnectionPool({
            'host': host,
            'port': port,
            'reconnect_retries': reconnect_retries,
            'reconnect_timeout': reconnect_timeout,
            'retries': retries
        },
            on_connect = self._initialize_connection,
            on_disconnect = self.on_disconnect,
            io_loop=self._io_loop,
            pool_size = pool_size,
        )

        self.async = _AsyncWrapper(self)

        self.queue = []
        self.current_cmd_line = None
        self.password = password
        self.db = db
        self.REPLY_MAP = dict_merge(
            string_keys_to_dict('AUTH BGREWRITEAOF BGSAVE DEL EXISTS EXPIRE HDEL HEXISTS '
                                'HMSET MOVE MSET MSETNX SAVE SETNX',
                                reply_to_bool),
            string_keys_to_dict('FLUSHALL FLUSHDB SELECT SET SETEX SHUTDOWN '
                                'RENAME RENAMENX WATCH UNWATCH',
                                make_reply_assert_msg('OK')),
            string_keys_to_dict('SMEMBERS SINTER SUNION SDIFF',
                                reply_set),
            string_keys_to_dict('HGETALL BRPOP BLPOP',
                                reply_dict_from_pairs),
            string_keys_to_dict('HGET',
                                reply_str),
            string_keys_to_dict('ZRANK ZREVRANK',
                                reply_int),
            string_keys_to_dict('ZSCORE ZINCRBY ZCOUNT ZCARD',
                                reply_int),
            string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE',
                                reply_zset),
            string_keys_to_dict('LLEN', reply_int),
                {'HMGET': reply_hmget},
                {'PING': make_reply_assert_msg('PONG')},
                {'LASTSAVE': reply_datetime},
                {'TTL': reply_ttl},
                {'INFO': reply_info},
                {'MULTI_PART': make_reply_assert_msg('QUEUED')},
            )

        self._waiting_callbacks = defaultdict(list)
        self._pipeline = None

    @process
    def _initialize_connection(self, connection, callback):
        """
            call after connection creation
        """
        log_blob.debug('initializing connection')
        with execution_context(callback) as ctx:
            cmds = []
            if self.password:
                cmds.append(CmdLine('AUTH', self.password))
            if self.db:
                cmds.append(CmdLine('SELECT', self.db))

            if cmds:
                try:
                    for cmd_line in cmds:
                        log_blob.debug('try to write to connection %s: %s', connection.idx, cmd_line)
                        write_res = yield async(connection.write)(self.format(
                            cmd_line.cmd,
                            *cmd_line.args,
                            **cmd_line.kwargs
                        ), before_init=True)
                        log_blob.debug('conn init, write_res: %s', write_res)
                except Exception, e:
                    connection.disconnect()
                    raise e

                for cmd_line in cmds:
                    data = yield async(connection.readline)()
                    response = yield self.process_data(connection, data, cmd_line)
                    result = self.format_reply(cmd_line, response)
                    log_blob.debug('got result %s on cmd %s', result, cmd_line)

            connection.is_initialized = True
            log_blob.debug('connection %s initialized', connection.idx)
            ctx.ret_call(True)

    #### new AsIO

    def __getattribute__(self, name):
        """
        Allow to remove AsyncWrapper from chain of calls
        """
        attr = super(Client, self).__getattribute__(name)
        if super(Client, self).__getattribute__('yield_mode'):
            func_name = getattr(attr, 'func_name', None)
            if func_name:
                if 'callbacks' in attr.func_code.co_varnames:
                    memoized = super(Client, self).__getattribute__('memoized')
                    if name not in memoized:
                        memoized[name] = async(attr, cbname='callbacks')
                    return memoized[name]
        return attr

    def __repr__(self):
        return '<Brukva client %s:%s>' % (self.host, self.port)

    def pipeline(self, transactional=False):
        if not self._pipeline:
            if  self.connection_pool.is_connected:
                self._pipeline = Pipeline(
                    db=self.db,
                    password=self.password,
                    io_loop = self._io_loop,
                    transactional=transactional
                )
                self._pipeline.connection_pool = self.connection_pool
            else:
                raise RedisError('Client must be connected befor creating pipeline')
        return self._pipeline

    #### connection

    def connect(self):
        self.connection_pool.connect()

    def disconnect(self):
        self.connection_pool.disconnect()

    #### formatting
    def on_disconnect(self, callback=None):
        error = ConnectionError("Socket closed on remote end")
        if callable(callback):
            callback(error)
        else:
            raise error
    ####

    def encode(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, unicode):
            return value.encode('utf-8')
            # pray and hope
        return str(value)

    def format(self, *tokens):
        cmds = []
        for t in tokens:
            e_t = self.encode(t)
            cmds.append('$%s\r\n%s\r\n' % (len(e_t), e_t))
        return '*%s\r\n%s' % (len(tokens), ''.join(cmds))
    def format_reply(self, cmd_line, data):
        if cmd_line.cmd not in self.REPLY_MAP:
            return data
        try:
            res = self.REPLY_MAP[cmd_line.cmd](data, *cmd_line.args, **cmd_line.kwargs)
        except Exception, e:
            raise ResponseError(
                'failed to format reply to %s, raw data: %s; err message: %s' %
                (cmd_line, data, e), cmd_line
            )
        return res

    ####

    def call_callbacks(self, callbacks, *args, **kwargs):
        for cb in callbacks:
            cb(*args, **kwargs)


    @process
    def execute_command(self, cmd, callbacks, *args, **kwargs):
        cmd_line = CmdLine(cmd, *args, **kwargs)
        with execution_context(callbacks) as ctx:
            if callbacks is None:
                callbacks = []
            elif not hasattr(callbacks, '__iter__'):
                callbacks = [callbacks]

            log_blob.debug('try to get connection for %s',  cmd_line)
            connection = yield async(self.connection_pool.request_connection)()
            log_blob.debug('got connection %s for %s cmd_line',connection.idx, cmd_line)

            try:
                write_res = yield async(connection.write)(self.format(cmd, *args, **kwargs))
            except Exception as e:
                connection.disconnect()
                ctx.ret_call(e)

            try:
                data = yield async(connection.readline)()
                if not data:
                    raise Exception('TODO: no data from connection %s->readline' % connection.idx)
                else:
                    response = yield self.process_data(connection, data, cmd_line)
                    result = self.format_reply(cmd_line, response)
                    log_blob.debug('got result %s on cmd %s with connection %s', result, cmd_line, connection.idx)
            finally:
                self.connection_pool.return_connection(connection)
            ctx.ret_call(result)

    @async
    @process
    def process_data(self, connection, original_data, cmd_line, callback):
        with execution_context(callback) as ctx:

            data = original_data[:-2] # strip \r\n

            if data == '$-1':
                response = None
            elif data == '*0' or data == '*-1':
                response = []
            else:
                if not len(data):
                    raise IOError('Disconnected')

                head, tail = data[0], data[1:]

                if head == '*':
                    response = yield self.consume_multibulk(connection, int(tail), cmd_line)
                elif head == '$':
                    response = yield self.consume_bulk(connection, int(tail)+2)
                elif head == '+':
                    response = tail
                elif head == ':':
                    response = int(tail)
                elif head == '-':
                    if tail.startswith('ERR'):
                        tail = tail[4:]
                    response = InternalRedisError(tail, cmd_line)
                else:
                    raise ResponseError('Unknown response type %s' % head, cmd_line)
            ctx.ret_call(response)

    @async
    @process
    def consume_multibulk(self, connection, length, cmd_line, callback):
        with execution_context(callback) as ctx:
            tokens = []
            data = yield async(connection.read_multibulk_reply)(length)
            if not data:
                raise ResponseError(
                    'Not enough data in response to %s, accumulated tokens: %s' %
                    (cmd_line, tokens), cmd_line
                )

            connection._consume_buffer = data
            while len(tokens) < length:
                data = yield async(connection.readline)()
                if not data:
                    raise ResponseError(
                        'Not enough data in response to %s, accumulated tokens: %s' %
                        (cmd_line, tokens), cmd_line
                    )
                token = yield self.process_data(connection, data, cmd_line) #FIXME error
                tokens.append( token )

            ctx.ret_call(tokens)

    @async
    @process
    def consume_bulk(self, connection, length, callback):
        with execution_context(callback) as ctx:
            data = yield async(connection.read)(length)
            if isinstance(data, Exception):
                raise data
            if not data:
                raise ResponseError('EmptyResponse')
            else:
                data = data[:-2]
            ctx.ret_call(data)

    ### MAINTENANCE
    def bgrewriteaof(self, callbacks=None):
        self.execute_command('BGREWRITEAOF', callbacks)

    def dbsize(self, callbacks=None):
        self.execute_command('DBSIZE', callbacks)

    def flushall(self, callbacks=None):
        self.execute_command('FLUSHALL', callbacks)

    def flushdb(self, callbacks=None):
        self.execute_command('FLUSHDB', callbacks)

    def ping(self, callbacks=None):
        self.execute_command('PING', callbacks)

    def info(self, callbacks=None):
        self.execute_command('INFO', callbacks)

    def select(self, db, callbacks=None):
        self.selected_db = db
        self.execute_command('SELECT', callbacks, db)

    def shutdown(self, callbacks=None):
        self.execute_command('SHUTDOWN', callbacks)

    def save(self, callbacks=None):
        self.execute_command('SAVE', callbacks)

    def bgsave(self, callbacks=None):
        self.execute_command('BGSAVE', callbacks)

    def lastsave(self, callbacks=None):
        self.execute_command('LASTSAVE', callbacks)

    def keys(self, pattern, callbacks=None):
        self.execute_command('KEYS', callbacks, pattern)

    ### BASIC KEY COMMANDS
    def append(self, key, value, callbacks=None):
        self.execute_command('APPEND', callbacks, key, value)

    def expire(self, key, ttl, callbacks=None):
        self.execute_command('EXPIRE', callbacks, key, ttl)

    def ttl(self, key, callbacks=None):
        self.execute_command('TTL', callbacks, key)

    def type(self, key, callbacks=None):
        self.execute_command('TYPE', callbacks, key)

    def randomkey(self, callbacks=None):
        self.execute_command('RANDOMKEY', callbacks)

    def rename(self, src, dst, callbacks=None):
        self.execute_command('RENAME', callbacks, src, dst)

    def renamenx(self, src, dst, callbacks=None):
        self.execute_command('RENAMENX', callbacks, src, dst)

    def move(self, key, db, callbacks=None):
        self.execute_command('MOVE', callbacks, key, db)

    def substr(self, key, start, end, callbacks=None):
        self.execute_command('SUBSTR', callbacks, key, start, end)

    def delete(self, key, callbacks=None):
        self.execute_command('DEL', callbacks, key)

    def set(self, key, value, callbacks=None):
        self.execute_command('SET', callbacks, key, value)

    def setex(self, key, ttl, value, callbacks=None):
        self.execute_command('SETEX', callbacks, key, ttl, value)

    def setnx(self, key, value, callbacks=None):
        self.execute_command('SETNX', callbacks, key, value)

    def mset(self, mapping, callbacks=None):
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        self.execute_command('MSET', callbacks, *items)

    def msetnx(self, mapping, callbacks=None):
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        self.execute_command('MSETNX', callbacks, *items)

    def get(self, key, callbacks=None):
        self.execute_command('GET', callbacks, key)

    def mget(self, keys, callbacks=None):
        self.execute_command('MGET', callbacks, *keys)

    def getset(self, key, value, callbacks=None):
        self.execute_command('GETSET', callbacks, key, value)

    def exists(self, key, callbacks=None):
        self.execute_command('EXISTS', callbacks, key)

    def sort(self, key, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, callbacks=None):
        if (start is not None and num is None) or (num is not None and start is None):
            raise ValueError("``start`` and ``num`` must both be specified")

        tokens = [key]
        if by is not None:
            tokens.append('BY')
            tokens.append(by)
        if start is not None and num is not None:
            tokens.append('LIMIT')
            tokens.append(start)
            tokens.append(num)
        if get is not None:
            tokens.append('GET')
            tokens.append(get)
        if desc:
            tokens.append('DESC')
        if alpha:
            tokens.append('ALPHA')
        if store is not None:
            tokens.append('STORE')
            tokens.append(store)
        return self.execute_command('SORT', callbacks, *tokens)

    ### COUNTERS COMMANDS
    def incr(self, key, callbacks=None):
        self.execute_command('INCR', callbacks, key)

    def decr(self, key, callbacks=None):
        self.execute_command('DECR', callbacks, key)

    def incrby(self, key, amount, callbacks=None):
        self.execute_command('INCRBY', callbacks, key, amount)

    def decrby(self, key, amount, callbacks=None):
        self.execute_command('DECRBY', callbacks, key, amount)

    ### LIST COMMANDS
    def blpop(self, keys, timeout=0, callbacks=None):
        tokens = list(keys)
        tokens.append(timeout)
        self.execute_command('BLPOP', callbacks, *tokens)

    def brpop(self, keys, timeout=0, callbacks=None):
        tokens = list(keys)
        tokens.append(timeout)
        self.execute_command('BRPOP', callbacks, *tokens)

    def brpoplpush(self, src, dst, timeout=1, callbacks=None):
        tokens = [src, dst, timeout]
        self.execute_command('BRPOPLPUSH', callbacks, *tokens)

    def lindex(self, key, index, callbacks=None):
        self.execute_command('LINDEX', callbacks, key, index)

    def llen(self, key, callbacks=None):
        self.execute_command('LLEN', callbacks, key)

    def lrange(self, key, start, end, callbacks=None):
        self.execute_command('LRANGE', callbacks, key, start, end)

    def lrem(self, key, value, num=0, callbacks=None):
        self.execute_command('LREM', callbacks, key, num, value)

    def lset(self, key, index, value, callbacks=None):
        self.execute_command('LSET', callbacks, key, index, value)

    def ltrim(self, key, start, end, callbacks=None):
        self.execute_command('LTRIM', callbacks, key, start, end)

    def lpush(self, key, value, callbacks=None):
        self.execute_command('LPUSH', callbacks, key, value)

    def rpush(self, key, value, callbacks=None):
        self.execute_command('RPUSH', callbacks, key, value)

    def lpop(self, key, callbacks=None):
        self.execute_command('LPOP', callbacks, key)

    def rpop(self, key, callbacks=None):
        self.execute_command('RPOP', callbacks, key)

    def rpoplpush(self, src, dst, callbacks=None):
        self.execute_command('RPOPLPUSH', callbacks, src, dst)

    ### SET COMMANDS
    def sadd(self, key, value, callbacks=None):
        self.execute_command('SADD', callbacks, key, value)

    def srem(self, key, value, callbacks=None):
        self.execute_command('SREM', callbacks, key, value)

    def scard(self, key, callbacks=None):
        self.execute_command('SCARD', callbacks, key)

    def spop(self, key, callbacks=None):
        self.execute_command('SPOP', callbacks, key)

    def smove(self, src, dst, value, callbacks=None):
        self.execute_command('SMOVE', callbacks, src, dst, value)

    def sismember(self, key, value, callbacks=None):
        self.execute_command('SISMEMBER', callbacks, key, value)

    def smembers(self, key, callbacks=None):
        self.execute_command('SMEMBERS', callbacks, key)

    def srandmember(self, key, callbacks=None):
        self.execute_command('SRANDMEMBER', callbacks, key)

    def sinter(self, keys, callbacks=None):
        self.execute_command('SINTER', callbacks, *keys)

    def sdiff(self, keys, callbacks=None):
        self.execute_command('SDIFF', callbacks, *keys)

    def sunion(self, keys, callbacks=None):
        self.execute_command('SUNION', callbacks, *keys)

    def sinterstore(self, keys, dst, callbacks=None):
        self.execute_command('SINTERSTORE', callbacks, dst, *keys)

    def sunionstore(self, keys, dst, callbacks=None):
        self.execute_command('SUNIONSTORE', callbacks, dst, *keys)

    def sdiffstore(self, keys, dst, callbacks=None):
        self.execute_command('SDIFFSTORE', callbacks, dst, *keys)

    ### SORTED SET COMMANDS
    def zadd(self, key, score, value, callbacks=None):
        self.execute_command('ZADD', callbacks, key, score, value)

    def zcard(self, key, callbacks=None):
        self.execute_command('ZCARD', callbacks, key)

    def zincrby(self, key, value, amount, callbacks=None):
        self.execute_command('ZINCRBY', callbacks, key, amount, value)

    def zrank(self, key, value, callbacks=None):
        self.execute_command('ZRANK', callbacks, key, value)

    def zrevrank(self, key, value, callbacks=None):
        self.execute_command('ZREVRANK', callbacks, key, value)

    def zrem(self, key, value, callbacks=None):
        self.execute_command('ZREM', callbacks, key, value)

    def zcount(self, key, start, end, offset=None, limit=None, with_scores=None, callbacks=None):
        tokens = [key, start, end]
        if offset is not None:
            tokens.append('LIMIT')
            tokens.append(offset)
            tokens.append(limit)
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZCOUNT', callbacks, *tokens)

    def zcard(self, key, callbacks=None):
        self.execute_command('ZCARD', callbacks, key)

    def zscore(self, key, value, callbacks=None):
        self.execute_command('ZSCORE', callbacks, key, value)

    def zrange(self, key, start, num, with_scores, callbacks=None):
        tokens = [key, start, num]
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZRANGE', callbacks, *tokens)

    def zrevrange(self, key, start, num, with_scores, callbacks=None):
        tokens = [key, start, num]
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZREVRANGE', callbacks, *tokens)

    def zrangebyscore(self, key, start, end, offset=None, limit=None, with_scores=False, callbacks=None):
        tokens = [key, start, end]
        if offset is not None:
            tokens.append('LIMIT')
            tokens.append(offset)
            tokens.append(limit)
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZRANGEBYSCORE', callbacks, *tokens)

    def zremrangebyrank(self, key, start, end, callbacks=None):
        self.execute_command('ZREMRANGEBYRANK', callbacks, key, start, end)

    def zremrangebyscore(self, key, start, end, callbacks=None):
        self.execute_command('ZREMRANGEBYSCORE', callbacks, key, start, end)

    def zinterstore(self, dest, keys, aggregate=None, callbacks=None):
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate, callbacks)

    def zunionstore(self, dest, keys, aggregate=None, callbacks=None):
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate, callbacks)

    def _zaggregate(self, command, dest, keys, aggregate, callbacks):
        tokens = [dest, len(keys)]
        if isinstance(keys, dict):
            items = keys.items()
            keys = [i[0] for i in items]
            weights = [i[1] for i in items]
        else:
            weights = None
        tokens.extend(keys)
        if weights:
            tokens.append('WEIGHTS')
            tokens.extend(weights)
        if aggregate:
            tokens.append('AGGREGATE')
            tokens.append(aggregate)
        return self.execute_command(command, callbacks, *tokens)

    ### HASH COMMANDS
    def hgetall(self, key, callbacks=None):
        self.execute_command('HGETALL', callbacks, key)

    def hmset(self, key, mapping, callbacks=None):
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        self.execute_command('HMSET', callbacks, key, *items)

    def hset(self, key, field, value, callbacks=None):
        self.execute_command('HSET', callbacks, key, field, value)

    def hsetnx(self, key, field, value, callbacks=None):
        self.execute_command('HSETNX', callbacks, key, field, value)

    def hget(self, key, field, callbacks=None):
        self.execute_command('HGET', callbacks, key, field)

    def hdel(self, key, field, callbacks=None):
        self.execute_command('HDEL', callbacks, key, field)

    def hlen(self, key, callbacks=None):
        self.execute_command('HLEN', callbacks, key)

    def hexists(self, key, field, callbacks=None):
        self.execute_command('HEXISTS', callbacks, key, field)

    def hincrby(self, key, field, amount=1, callbacks=None):
        self.execute_command('HINCRBY', callbacks, key, field, amount)

    def hkeys(self, key, callbacks=None):
        self.execute_command('HKEYS', callbacks, key)

    def hmget(self, key, fields, callbacks=None):
        self.execute_command('HMGET', callbacks, key, *fields)

    def hvals(self, key, callbacks=None):
        self.execute_command('HVALS', callbacks, key)

    ### CAS
    def watch(self, key, callbacks=None):
        self.execute_command('WATCH', callbacks, key)

    def unwatch(self, callbacks=None):
        self.execute_command('UNWATCH', callbacks)


class Pipeline(Client):
    def __init__(self, transactional, *args, **kwargs):
        super(Pipeline, self).__init__(*args, **kwargs)
        self.transactional = transactional
        self.command_stack = []

    def execute_command(self, cmd, callbacks, *args, **kwargs):
        self.command_stack.append(CmdLine(cmd, *args, **kwargs))

    def discard(self): # actually do nothing with redis-server, just flush command_stack
        self.command_stack = []

    @process
    def execute(self, callbacks):
        with execution_context(callbacks) as ctx:
            command_stack = self.command_stack
            self.command_stack = []

            if callbacks is None:
                callbacks = []
            elif not hasattr(callbacks, '__iter__'):
                callbacks = [callbacks]

            if self.transactional:
                command_stack = [CmdLine('MULTI')] + command_stack + [CmdLine('EXEC')]

            request = format_pipeline_request(command_stack)

            log_blob.debug('try to get connection for %r', request)
            connection = yield async(self.connection_pool.request_connection)()
            log_blob.debug('got connection %s for %r request', connection.idx, request)
            try:
                write_res = yield async(connection.write)(request)
                log_blob.debug('conn  write res: %r', write_res)
            except Exception, e:
                connection.disconnect()
                raise e

            responses = []
            total = len(command_stack)
            cmds = iter(command_stack)

            try:
                while len(responses) < total:
                    data = yield async(connection.readline)()
                    if not data:
                        raise ResponseError('Not enough data after EXEC')
                    try:
                        cmd_line = cmds.next()
                        if self.transactional and cmd_line.cmd != 'EXEC':
                            response = yield self.process_data(connection, data, CmdLine('MULTI_PART'))
                        else:
                            response = yield self.process_data(connection, data, cmd_line)
                        responses.append(response)
                    except Exception,e :
                        responses.append(e)
            finally:
                self.connection_pool.return_connection(connection)

            def format_replies(cmd_lines, responses):
                results = []
                for cmd_line, response in zip(cmd_lines, responses):
                    try:
                        results.append(self.format_reply(cmd_line, response))
                    except Exception, e:
                        results.append(e)
                return results

            if self.transactional:
                command_stack = command_stack[:-1]
                responses = responses[-1] # actual data only from EXEC command
                #FIXME:  assert all other responses to be 'QUEUED'
                log.info('responses %s', responses)
                results = format_replies(command_stack[1:], responses)
                log_blob.debug('on request %r pipe results %s', request, results)
            else:
                results = format_replies(command_stack, responses)

            ctx.ret_call(results)
