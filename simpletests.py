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


def test_msg():
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    push.bind('inproc://push')
    pull.connect('inproc://push')
    zeronimo.zmq_send(push, 1)
    assert zeronimo.zmq_recv(pull) == 1
    zeronimo.zmq_send(push, 'doctor')
    assert zeronimo.zmq_recv(pull) == 'doctor'
    zeronimo.zmq_send(push, {'doctor': 'who'})
    assert zeronimo.zmq_recv(pull) == {'doctor': 'who'}
    zeronimo.zmq_send(push, ['doctor', 'who'])
    assert zeronimo.zmq_recv(pull) == ['doctor', 'who']
    zeronimo.zmq_send(push, zeronimo.ZeronimoException)
    assert zeronimo.zmq_recv(pull) == zeronimo.ZeronimoException
    zeronimo.zmq_send(push, zeronimo.ZeronimoException('Allons-y'))
    assert isinstance(zeronimo.zmq_recv(pull), zeronimo.ZeronimoException)
    push.close()
    pull.close()
