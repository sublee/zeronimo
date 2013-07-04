# -*- coding: utf-8 -*-
import gevent
import zmq.green as zmq

import zeronimo

from conftest import link_sockets


def test_running():
    from zeronimo.components import Runner
    class NullRunner(Runner):
        def run(self):
            pass
    runner = NullRunner()
    assert not runner.is_running()
    runner.start()
    assert runner.is_running()
    runner.wait()
    assert not runner.is_running()


def test_messaging(ctx, addr, topic):
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    link_sockets(addr, push, [pull])
    for t in [None, topic]:
        zeronimo.send(push, 1, topic=t)
        assert zeronimo.recv(pull) == 1
        zeronimo.send(push, 'doctor', topic=t)
        assert zeronimo.recv(pull) == 'doctor'
        zeronimo.send(push, {'doctor': 'who'}, topic=t)
        assert zeronimo.recv(pull) == {'doctor': 'who'}
        zeronimo.send(push, ['doctor', 'who'], topic=t)
        assert zeronimo.recv(pull) == ['doctor', 'who']
        zeronimo.send(push, Exception, topic=t)
        assert zeronimo.recv(pull) == Exception
        zeronimo.send(push, Exception('Allons-y'), topic=t)
        assert isinstance(zeronimo.recv(pull), Exception)
