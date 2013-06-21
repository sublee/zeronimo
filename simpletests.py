# -*- coding: utf-8 -*-
import functools
import os

import gevent
from gevent import joinall, spawn, Timeout
import pytest
import zmq.green as zmq

from conftest import (
    app, autowork, ctx, green, run_device, sync_pubsub,
    patch_worker_to_be_slow)
import zeronimo


@autowork
def test_msg(addr, prefix):
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    push.bind(addr)
    pull.connect(addr)
    for p in ['', prefix]:
        zeronimo.zmq_send(push, 1, prefix=p)
        assert zeronimo.zmq_recv(pull) == 1
        zeronimo.zmq_send(push, 'doctor', prefix=p)
        assert zeronimo.zmq_recv(pull) == 'doctor'
        zeronimo.zmq_send(push, {'doctor': 'who'}, prefix=p)
        assert zeronimo.zmq_recv(pull) == {'doctor': 'who'}
        zeronimo.zmq_send(push, ['doctor', 'who'], prefix=p)
        assert zeronimo.zmq_recv(pull) == ['doctor', 'who']
        zeronimo.zmq_send(push, Exception, prefix=p)
        assert zeronimo.zmq_recv(pull) == Exception
        zeronimo.zmq_send(push, Exception('Allons-y'), prefix=p)
        assert isinstance(zeronimo.zmq_recv(pull), Exception)
    push.close()
    pull.close()
