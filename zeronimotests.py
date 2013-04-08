# -*- coding: utf-8 -*-
import os

from gevent import joinall, spawn, Timeout
import pytest

from conftest import autowork, Application, inproc, ipc, tcp, pgm, epgm
import zeronimo


def test_running():
    class Runner(zeronimo.Base):
        def reset_sockets(self):
            pass
        def run(self):
            assert self.running
    runner = Runner()
    assert not runner.running
    runner.run()
    assert not runner.running


@autowork
def test_tunnel(customer, worker):
    yield [worker]
    assert len(customer.tunnels) == 0
    with customer.link_workers([worker]) as tunnel:
        assert len(customer.tunnels) == 1
    assert len(customer.tunnels) == 0
    with customer.link_workers([worker]) as tunnel1, \
         customer.link_workers([worker]) as tunnel2:
        assert not customer.running
        assert len(customer.tunnels) == 2
        tunnel1.add(0, 0)
        assert customer.running
    # should clean up
    assert not customer.running
    assert len(customer.tunnels) == 0


@autowork
def test_return(customer, worker):
    yield [worker]
    with customer.link_workers([worker]) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'
        assert tunnel.add(2, 2) == 'cutie'
        assert tunnel.add(3, 3) == 'cutie'
        assert tunnel.add(4, 4) == 'cutie'
        assert tunnel.add(5, 5) == 'cutie'
        assert tunnel.add(6, 6) == 'xoxoxoxoxoxo cutie'
        assert tunnel.add(42, 12) == 54


@autowork
def test_yield(customer, worker):
    yield [worker]
    with customer.link_workers([worker]) as tunnel:
        assert len(list(tunnel.jabberwocky())) == 4
        assert list(tunnel.xrange()) == [0, 1, 2, 3, 4]
        view = tunnel.dict_view()
        assert view.next() == 0
        assert view.next() == 1
        assert view.next() == 2
        assert view.next() == 3
        assert view.next() == 4
        with pytest.raises(StopIteration):
            view.next()
        assert list(tunnel.dont_yield()) == []


@autowork
def test_raise(customer, worker):
    yield [worker]
    with customer.link_workers([worker]) as tunnel:
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
        rocket_launching = tunnel.launch_rocket()
        assert rocket_launching.next() == 3
        assert rocket_launching.next() == 2
        assert rocket_launching.next() == 1
        with pytest.raises(RuntimeError):
            rocket_launching.next()


@autowork
def test_2to1(customer1, customer2, worker):
    yield [worker]
    def test(tunnel):
        assert tunnel.add(1, 1) == 'cutie'
        assert len(list(tunnel.jabberwocky())) == 4
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
    with customer1.link_workers([worker]) as tunnel1, \
         customer2.link_workers([worker]) as tunnel2:
        joinall([spawn(test, tunnel1), spawn(test, tunnel2)])


@autowork
def test_1to2(customer, worker1, worker2):
    yield [worker1, worker2]
    with customer.link_workers([worker1, worker2], as_task=True) as tunnel:
        task1 = tunnel.add(1, 1)
        task2 = tunnel.add(2, 2)
        assert task1() == 'cutie'
        assert task2() == 'cutie'
        assert task1.worker_addr != task2.worker_addr


@autowork
def test_fanout(customer, worker1, worker2):
    yield [worker1, worker2]
    with customer.link_workers([worker1, worker2]) as tunnel:
        assert list(tunnel.rycbar123()) == \
               'run, you clever boy; and remember.'.split()
        for rycbar123 in tunnel(fanout=True).rycbar123():
            assert rycbar123.next() == 'run,'
            assert rycbar123.next() == 'you'
            assert rycbar123.next() == 'clever'
            assert rycbar123.next() == 'boy;'
            assert rycbar123.next() == 'and'
            assert rycbar123.next() == 'remember.'
        with pytest.raises(ZeroDivisionError):
            tunnel(fanout=True).zero_div()
        failures = tunnel(fanout=True, as_task=True).zero_div()
        assert len(failures) == 2
        with pytest.raises(ZeroDivisionError):
            failures[0]()
        with pytest.raises(ZeroDivisionError):
            failures[1]()


@autowork
def test_slow(customer, worker):
    yield [worker]
    with customer.link_workers([worker]) as tunnel:
        with pytest.raises(Timeout):
            with Timeout(0.1):
                tunnel.sleep()
        assert tunnel.sleep() == 'slept'


@autowork
def test_reject(customer, worker1, worker2):
    yield [worker1, worker2]
    with customer.link_workers([worker1, worker2]) as tunnel:
        assert len(list(tunnel(fanout=True).simple())) == 2
        worker2.reject_all()
        assert len(list(tunnel(fanout=True).simple())) == 1
        worker2.accept_all()
        assert len(list(tunnel(fanout=True).simple())) == 2


@autowork
def test_topic(customer, worker1, worker2):
    yield [worker1, worker2]
    fanout_addrs = list(worker1.fanout_addrs) + list(worker2.fanout_addrs)
    fanout_topic = worker1.fanout_topic
    with customer.link(None, fanout_addrs, fanout_topic) as tunnel:
        assert len(list(tunnel(fanout=True).simple())) == 2
        worker2.subscribe('stop')
        assert len(list(tunnel(fanout=True).simple())) == 1
        worker1.subscribe('stop')
        with pytest.raises(zeronimo.ZeronimoError):
            tunnel(fanout=True).simple()
        worker1.subscribe(fanout_topic)
        assert len(list(tunnel(fanout=True).simple())) == 1
        worker2.subscribe(fanout_topic)
        assert len(list(tunnel(fanout=True).simple())) == 2


@autowork
def test_link_to_addrs(customer, worker):
    yield [worker]
    with customer.link(worker.addrs, worker.fanout_addrs,
                       worker.fanout_topic) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'


@autowork
def test_ipc():
    app = Application()
    worker1 = zeronimo.Worker(app)
    worker2 = zeronimo.Worker(app)
    customer1 = zeronimo.Customer()
    customer2 = zeronimo.Customer()
    if not os.path.isdir('_feeds'):
        os.mkdir('_feeds')
    worker1.bind('ipc://_feeds/worker1')
    worker1.bind_fanout('ipc://_feeds/worker1_fanout')
    worker2.bind('ipc://_feeds/worker2')
    worker2.bind_fanout('ipc://_feeds/worker2_fanout')
    customer1.bind('ipc://_feeds/customer1')
    customer2.bind('ipc://_feeds/customer2')
    assert not os.path.exists('_feeds/worker1')
    assert not os.path.exists('_feeds/worker1_fanout')
    assert not os.path.exists('_feeds/worker2')
    assert not os.path.exists('_feeds/worker2_fanout')
    assert not os.path.exists('_feeds/customer1')
    assert not os.path.exists('_feeds/customer2')
    yield [worker1, worker2]
    assert os.path.exists('_feeds/worker1')
    assert os.path.exists('_feeds/worker1_fanout')
    assert os.path.exists('_feeds/worker2')
    assert os.path.exists('_feeds/worker2_fanout')
    with customer1.link_workers([worker1]) as tunnel1:
        assert os.path.exists('_feeds/customer1')
        assert tunnel1.simple() == 'ok'
    assert not os.path.exists('_feeds/customer1')
    assert os.path.exists('_feeds/worker1')
    assert os.path.exists('_feeds/worker1_fanout')
    assert os.path.exists('_feeds/worker2')
    assert os.path.exists('_feeds/worker2_fanout')
    with customer1.link_workers([worker1, worker2]) as tunnel1, \
         customer2.link_workers([worker1, worker2]) as tunnel2:
        assert os.path.exists('_feeds/customer1')
        assert os.path.exists('_feeds/customer2')
        assert tunnel1.simple() == 'ok'
        assert tunnel2.simple() == 'ok'
        assert list(tunnel1(fanout=True).simple()) == ['ok', 'ok']
        assert list(tunnel2(fanout=True).simple()) == ['ok', 'ok']
    assert not os.path.exists('_feeds/customer1')
    assert not os.path.exists('_feeds/customer2')


@autowork
def test_tcp():
    # assign
    app = Application()
    worker1 = zeronimo.Worker(app)
    worker2 = zeronimo.Worker(app)
    customer1 = zeronimo.Customer()
    customer2 = zeronimo.Customer()
    # bind
    worker1.bind(tcp())
    worker1.bind_fanout(tcp())
    worker2.bind(tcp())
    worker2.bind_fanout(tcp())
    customer1.bind(tcp())
    customer2.bind(tcp())
    # start
    yield [worker1, worker2]
    # test
    with customer1.link_workers([worker1]) as tunnel1:
        assert tunnel1.simple() == 'ok'
    with customer1.link_workers([worker1, worker2]) as tunnel1, \
         customer2.link_workers([worker1, worker2]) as tunnel2:
        assert tunnel1.simple() == 'ok'
        assert tunnel2.simple() == 'ok'
        assert list(tunnel1(fanout=True).simple()) == ['ok', 'ok']
        assert list(tunnel2(fanout=True).simple()) == ['ok', 'ok']


@autowork
def test_epgm():
    # assign
    app = Application()
    worker1 = zeronimo.Worker(app)
    worker2 = zeronimo.Worker(app)
    customer1 = zeronimo.Customer()
    customer2 = zeronimo.Customer()
    fanout_addr = epgm()
    worker1.bind(tcp())
    worker1.bind_fanout(fanout_addr)
    worker2.bind(tcp())
    worker2.bind_fanout(fanout_addr)
    customer1.bind(tcp())
    customer2.bind(tcp())
    yield [worker1, worker2]
    with customer1.link_workers([worker1]) as tunnel1:
        assert tunnel1.simple() == 'ok'
    with customer1.link_workers([worker1, worker2]) as tunnel1, \
         customer2.link_workers([worker1, worker2]) as tunnel2:
        assert tunnel1.simple() == 'ok'
        assert tunnel2.simple() == 'ok'
        assert list(tunnel1(fanout=True).simple()) == ['ok', 'ok']
        assert list(tunnel2(fanout=True).simple()) == ['ok', 'ok']
