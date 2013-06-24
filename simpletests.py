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
    app, autowork, ctx, green, run_device, sync_pubsub,
    patch_worker_to_be_slow)
import zeronimo


import psutil
ps = psutil.Process(os.getpid())


def zmq_link(addr, socket_to_bind, sockets_to_connect):
    socket_to_bind.bind(addr)
    for sock in sockets_to_connect:
        sock.connect(addr)


def wait_to_close(addr):
    protocol, location = addr.split('://', 1)
    if protocol == 'inproc':
        return
    elif protocol == 'ipc':
        still_exists = lambda: os.path.exists(location)
    elif protocol == 'tcp':
        host, port = location.split(':')
        port = int(port)
        def still_exists():
            for conn in ps.get_connections():
                if conn.local_address == (host, port):
                    return True
            return False
    while still_exists():
        gevent.sleep(0.001)


@autowork
def test_msg(addr, prefix):
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    zmq_link(addr, push, [pull])
    for p in ['', prefix]:
        t = time.time()
        zeronimo.send(push, 1, prefix=p)
        assert zeronimo.recv(pull) == 1
        print (time.time() - t) * 10 ** 3
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
    protocol, location = addr.split('://', 1)
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    zmq_link(addr, pull, [push])
    zeronimo.send(push, 'expected')
    assert zeronimo.recv(pull) == 'expected'
    # close and reopen
    print 'close'
    while 1:
        pull.close()
        wait_to_close(addr)
        if os.path.exists(location):
            print 'closed but exists'
        pull = ctx.socket(zmq.PULL)
        zmq_link(addr, pull, [push])
        if not os.path.exists(location):
            print 'opened but not exist'
        print 'send', addr
        t = time.time()
        zeronimo.send(push, 'expected')
        print 'recv', addr
        assert zeronimo.recv(pull) == 'expected'
        print (time.time() - t) * 10 ** 3
    # close all
    print 'close'
    pull.close()
    push.close()
    wait_to_close(addr)


'''
    # using poller
    pull.close()
    wait_to_close(addr)
    pull = ctx.socket(zmq.PULL)
    yield zmq_link(addr, pull, [push])
    poller = zmq.Poller()
    poller.register(pull)
    polling = gevent.spawn(poller.poll)
    zeronimo.send(push, 'expected')
    assert polling.get() == [(pull, zmq.POLLIN)]
    assert zeronimo.recv(pull) == 'expected'
    pull.close()
    push.close()
    wait_to_close(addr)
'''
