# -*- coding: utf-8 -*-
import gevent
from gevent import joinall, spawn
import pytest
import zmq.green as zmq

import zeronimo

from conftest import link_sockets


def test_running():
    from zeronimo.components import Runnable
    class NullRunner(Runnable):
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


def test_fixtures(worker, push, pub, collector, addr1, addr2, ctx):
    assert isinstance(worker, zeronimo.Worker)
    assert len(worker.sockets) == 2
    assert push.type == zmq.PUSH
    assert pub.type == zmq.PUB
    assert isinstance(collector, zeronimo.Collector)
    assert addr1 != addr2
    assert isinstance(ctx, zmq.Context)
    assert worker.is_running()
    assert collector.is_running()


def test_nowait(worker, push):
    customer = zeronimo.Customer(push)
    assert worker.obj.counter['zeronimo'] == 0
    customer.zeronimo()
    customer.zeronimo()
    customer.zeronimo()
    customer.zeronimo()
    customer.zeronimo()
    assert worker.obj.counter['zeronimo'] == 0
    gevent.sleep(0.01)
    assert worker.obj.counter['zeronimo'] == 5


def test_fanout_nowait(worker, worker2, worker3, worker4, worker5, pub, topic):
    customer = zeronimo.Customer(pub)
    assert worker.obj.counter['zeronimo'] == 0
    v = customer[topic].zeronimo()
    assert worker.obj.counter['zeronimo'] == 0
    gevent.sleep(0.01)
    assert worker.obj.counter['zeronimo'] == 5


def test_return(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert customer.zeronimo() == 'zeronimo'
    assert customer.add(100, 200) == 300
    assert customer.add('100', '200') == '100200'


def test_yield(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert ' '.join(customer.rycbar123()) == \
           'run, you clever boy; and remember.'
    assert list(customer.dont_yield()) == []


def test_raise(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    with pytest.raises(ZeroDivisionError):
        customer.zero_div()
    g = customer.rycbar123_and_zero_div()
    assert next(g) == 'run,'
    assert next(g) == 'you'
    assert next(g) == 'clever'
    assert next(g) == 'boy;'
    assert next(g) == 'and'
    assert next(g) == 'remember.'
    with pytest.raises(ZeroDivisionError):
        next(g)


def test_iterator(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert list(customer.xrange(1, 100, 10)) == range(1, 100, 10)
    assert set(customer.dict_view(1, 100, 10)) == set(range(1, 100, 10))


def test_fanout_return(worker1, worker2, collector, pub, topic):
    customer = zeronimo.Customer(pub, collector)
    assert customer[topic].zeronimo() == ['zeronimo', 'zeronimo']
    assert customer[topic].add(100, 200) == [300, 300]
    assert customer[topic].add('100', '200') == ['100200', '100200']


def test_fanout_yield(worker1, worker2, collector, pub, topic):
    customer = zeronimo.Customer(pub, collector)
    for g in customer[topic].rycbar123():
        assert next(g) == 'run,'
        assert next(g) == 'you'
        assert next(g) == 'clever'
        assert next(g) == 'boy;'
        assert next(g) == 'and'
        assert next(g) == 'remember.'


def test_fanout_raise(worker1, worker2, collector, pub, topic):
    customer = zeronimo.Customer(pub, collector)
    with pytest.raises(ZeroDivisionError):
        customer[topic].zero_div()


def test_2to1(worker, collector, push1, push2):
    customer1 = zeronimo.Customer(push1, collector)
    customer2 = zeronimo.Customer(push2, collector)
    def test(customer):
        assert customer.add(1, 1) == 2
        assert len(list(customer.rycbar123())) == 6
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
    joinall([spawn(test, customer1), spawn(test, customer2)])


def test_1to2(worker1, worker2, task_collector, push):
    customer = zeronimo.Customer(push, task_collector)
    task1 = customer.add(1, 1)
    task2 = customer.add(2, 2)
    assert task1() == 2
    assert task2() == 4
    assert task1.worker_info != task2.worker_info
