# -*- coding: utf-8 -*-
import pdb
import functools
import os
import time

import gevent
from gevent import Timeout
import pytest
import zmq.green as zmq

from conftest import (
    autowork, ctx,
    patch_worker_to_be_slow)
import zeronimo


import psutil
ps = psutil.Process(os.getpid())


@autowork
def test_msg(addr, prefix):
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    link_sockets(addr, push, [pull])
    for p in ['', prefix]:
        t = time.time()
        zeronimo.send(push, 1, prefix=p)
        assert zeronimo.recv(pull) == 1
        zeronimo.send(push, 'doctor', prefix=p)
        assert zeronimo.recv(pull) == 'doctor'
        zeronimo.send(push, {'doctor': 'who'}, prefix=p)
        assert zeronimo.recv(pull) == {'doctor': 'who'}
        zeronimo.send(push, ['doctor', 'who'], prefix=p)
        assert zeronimo.recv(pull) == ['doctor', 'who']
        zeronimo.send(push, Exception, prefix=p)
        assert zeronimo.recv(pull) == Exception
        zeronimo.send(push, Exception('Allons-y'), prefix=p)
        assert isinstance(zeronimo.recv(pull), Exception)
    push.close()
    pull.close()
    wait_to_close(addr)


@autowork
def test_reopen(addr):
    for x in xrange(100):
        pull = ctx.socket(zmq.PULL)
        push = ctx.socket(zmq.PUSH)
        link_sockets(addr, pull, [push])
        zeronimo.send(push, 'expected')
        assert zeronimo.recv(pull) == 'expected'
        pull.close()
        push.close()
        wait_to_close(addr)


@autowork
def test_reopen_and_poll(addr):
    for x in xrange(100):
        pull = ctx.socket(zmq.PULL)
        push = ctx.socket(zmq.PUSH)
        link_sockets(addr, pull, [push])
        poller = zmq.Poller()
        poller.register(pull)
        polling = gevent.spawn(poller.poll)
        zeronimo.send(push, 'expected')
        assert polling.get() == [(pull, zmq.POLLIN)]
        assert zeronimo.recv(pull) == 'expected'
        pull.close()
        push.close()
        wait_to_close(addr)
