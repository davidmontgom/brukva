# -*- coding: utf-8 -*-
import logging
import sys
from tornado.iostream import IOStream, _merge_prefix

class BrukvaStream(IOStream):

    def __init__(self, socket, io_loop=None, max_buffer_size=104857600, read_chunk_size=4096):
        super(BrukvaStream, self).__init__(socket, io_loop, max_buffer_size, read_chunk_size)
        self._read_delimiter_times = None

    def read_until_times(self, delimiter, times,  callback):
        assert type(times) == int, 'Times should be integer'
        self._read_delimiter_times = times
        super(BrukvaStream, self).read_until(delimiter, callback)

    def read_bytes(self, num_bytes, callback):
        super(BrukvaStream, self).read_bytes(num_bytes, callback)

    def _read_from_buffer(self):
        if self._read_delimiter and self._read_delimiter_times:
            _merge_prefix(self._read_buffer, sys.maxint)
            chunks = self._read_buffer[0].split(self._read_delimiter, self._read_delimiter_times)[:-1]
#            logging.debug("READ BUFFER % s" % self._read_buffer)
#            logging.debug("Chunks: %s " % chunks)
            if chunks:
                chunks_str = '\r\n'.join(chunks)
                callback = self._read_callback
                delimiter_len = len(self._read_delimiter)
                self._read_callback = None
                self._read_delimiter = None
                self._read_delimiter_times = None
                self._run_callback(callback,
                                   self._consume(len(chunks_str)+delimiter_len))

                return True

            return False
        else:
            return super(BrukvaStream, self)._read_from_buffer()





