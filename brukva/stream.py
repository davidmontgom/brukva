# -*- coding: utf-8 -*-
import logging
import sys
from tornado import stack_context
from tornado.iostream import IOStream, _merge_prefix

class BrukvaStream(IOStream):

    def __init__(self, socket, io_loop=None, max_buffer_size=104857600, read_chunk_size=4096):
        super(BrukvaStream, self).__init__(socket, io_loop, max_buffer_size, read_chunk_size)
        self._read_delimiter_times = None
        self._read_multibulk_answers = None

    def read_until_times(self, delimiter, times,  callback):
        assert type(times) == int, 'Times should be integer'
        self._read_delimiter_times = times
        super(BrukvaStream, self).read_until(delimiter, callback)

    def read_bytes(self, num_bytes, callback, streaming_callback=None):
        super(BrukvaStream, self).read_bytes(num_bytes, callback, streaming_callback)

    def read_multibulk(self, num_answers, callback):
        """Call callback when we read the multibulk answer."""
        assert not self._read_callback, "Already reading"
        self._read_multibulk_answers = int(num_answers)
        self._read_callback = stack_context.wrap(callback)
        while True:
            # See if we've already got the data from a previous read
            if self._read_from_buffer():
                return
            self._check_closed()
            if not self._read_to_buffer():
                break
        self._add_io_state(self.io_loop.READ)

    def _read_from_buffer(self):
        if self._read_delimiter and self._read_delimiter_times:
            _merge_prefix(self._read_buffer, sys.maxint)
            chunks = self._read_buffer[0].split(self._read_delimiter, self._read_delimiter_times)[:-1]
            if chunks:
                chunks_str = self._read_delimiter.join(chunks)
                callback = self._read_callback
                delimiter_len = len(self._read_delimiter)
                self._read_callback = None
                self._read_delimiter = None
                self._read_delimiter_times = None
                self._run_callback(callback,self._consume(len(chunks_str)+delimiter_len))
                return True

            return False
        elif self._read_multibulk_answers:
            _merge_prefix(self._read_buffer, sys.maxint)
            delimiter = '\r\n'
            # multiple by two cause 1 answer assume to have two '\r\n'
            chunks = self._read_buffer[0].split(delimiter, self._read_multibulk_answers*2)[:-1]
            if chunks:
                # but if answer is nil, then we got one '\r\n'
                # so we must remove unnecessary chunks from next redis responses
                nil_answers = 0
                for chunk in chunks:
                    if '$-1' in chunk:
                        nil_answers += 1
                chunks = chunks[:-nil_answers]
                
                chunks_str = delimiter.join(chunks)
                callback = self._read_callback
                self._read_multibulk_answers = None
                self._read_callback = None
                self._run_callback(callback, self._consume(len(chunks_str)+len(delimiter)))
                return True
            return False

        else:
            return super(BrukvaStream, self)._read_from_buffer()





