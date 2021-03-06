# -*- coding: utf-8 -*-
import collections
from sys import stdin
import mock
from nose.tools import eq_
from tornado.testing import AsyncTestCase
import unittest2
from brukva.adisp import process, async
from brukva.stream import BrukvaStream

class TestBrukvaStream(AsyncTestCase):

    def setUp(self):
        super(TestBrukvaStream, self).setUp()
        self.socket = mock.Mock()
        self.socket.fileno.return_value = stdin
        self.stream = BrukvaStream(self.socket, io_loop=self.io_loop)

    def test_read_bytes(self):
        string = 'hello world'
        len = 5
        self.socket.recv.return_value = string
        self.stream.read_bytes(len, self.stop)
        response = self.wait()
        eq_(response, string[:5])

    def test_read_until_times(self):
        string = 'hash:dsd:dsda:dsd'
        times = 2
        self.socket.recv.return_value = string
        delimiter = ':'
        self.stream.read_until_times(delimiter,times, self.stop)
        response = self.wait()
        eq_(response, ':'.join(string.split(delimiter, times)[:-1])+':')

    def test_read_multibulk(self):
        # test reply with nil
        num_answers = 3
        answer = '$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n'
        string = answer + '*1\r\n$3\r\nboo\r\n'
        self.socket.recv.return_value = string
        self.stream.read_multibulk(num_answers, self.stop)
        response = self.wait()
        eq_(response, answer)
        self.stream._read_buffer  = collections.deque()

        #test reply without nil
        num_answers = 8
        answers_list = [':1', ':1', '$1', '1', '$1', '2', ':0', ':1', '*4', '$1', 'a', '$1', '1', '$1', 'b', '$1', '2', '*2', '$1', 'a', '$1', 'b']
        answer = '\r\n'.join(answers_list) + '\r\n'
        string = answer
        self.socket.recv.return_value = string
        self.stream.read_multibulk(num_answers, self.stop)
        response = self.wait()
        eq_(response, answer)


if __name__ == '__main__':
    unittest2.main()
