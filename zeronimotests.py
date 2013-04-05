import functools
from itertools import chain
import os
import textwrap
import uuid

from decorator import decorator
import gevent
from gevent import joinall, killall, spawn, Timeout
import pytest
import zmq.green as zmq

import zeronimo


zmq_context = zmq.Context()
gevent.hub.get_hub().print_exception = lambda *a, **k: 'do not print exception'


@decorator
def green(f, *args, **kwargs):
    return spawn(f, *args, **kwargs).get()


def busywait(while_, until=False, timeout=None):
    """Sleeps while the ``while_`` function returns ``True``."""
    until = bool(until)
    with gevent.Timeout(timeout):
        while until != bool(while_()):
            gevent.sleep(0.001)


def is_connectable(addr, socket_type, context=zmq_context):
    """Checks that the address is connectable."""
    try:
        context.socket(socket_type).connect(addr)
    except zmq.ZMQError:
        return False
    else:
        return True


def inproc():
    return 'inproc://{0}'.format(zeronimo.uuid_str())


def start_workers(workers):
    waits = []
    for worker in workers:
        spawn(worker.run)
        for addr in chain(worker.addrs, worker.fanout_addrs):
            until = lambda: is_connectable(addr, zmq.PUSH)
            waits.append(spawn(busywait, until, until=True, timeout=1))
    joinall(waits)


class Application(object):

    @zeronimo.remote
    def add(self, a, b):
        """Koreans' mathematical addition."""
        if a == b:
            if 1 <= a < 6:
                return 'cutie'
            elif a == 6:
                return 'xoxoxoxoxoxo cutie'
        return a + b

    @zeronimo.remote
    def jabberwocky(self):
        yield 'Twas brillig, and the slithy toves'
        yield 'Did gyre and gimble in the wabe;'
        yield 'All mimsy were the borogoves,'
        yield 'And the mome raths outgrabe.'

    @zeronimo.remote
    def xrange(self):
        return xrange(5)

    @zeronimo.remote
    def dict_view(self):
        return dict(zip(xrange(5), xrange(5))).viewkeys()

    @zeronimo.remote
    def dont_yield(self):
        if False:
            yield 'it should\'t be sent'
            assert 0

    @zeronimo.remote
    def zero_div(self):
        0/0      /0/0 /0/0    /0/0   /0/0/0/0   /0/0
        0/0  /0  /0/0 /0/0    /0/0 /0/0    /0/0     /0
        0/0/0/0/0/0/0 /0/0/0/0/0/0 /0/0    /0/0   /0
        0/0/0/0/0/0/0 /0/0    /0/0 /0/0    /0/0
        0/0  /0  /0/0 /0/0    /0/0   /0/0/0/0     /0

    @zeronimo.remote
    def launch_rocket(self):
        yield 3
        yield 2
        yield 1
        raise RuntimeError('Launch!')

    @zeronimo.remote
    def rycbar123(self):
        for word in 'run, you clever boy; and remember.'.split():
            yield word

    @zeronimo.remote
    def sleep(self):
        gevent.sleep(0.1)
        return 'slept'


# fixtures


for x in xrange(4):
    exec(textwrap.dedent('''
    @pytest.fixture
    def worker{x}():
        app = Application()
        return zeronimo.Worker(app, inproc(), inproc(), '',
                               context=zmq_context)
    @pytest.fixture
    def customer{x}():
        return zeronimo.Customer(inproc(), context=zmq_context)
    ''').format(x=x if x else ''))


# tests


def test_remote_function_collection():
    class App(object):
        @zeronimo.remote
        def foo(self):
            return 'foo-%s' % id(self)
        @zeronimo.remote
        def bar(self):
            return 'bar-%s' % id(self)
        @zeronimo.remote
        def baz(self):
            yield 'baz-%s-begin' % id(self)
            yield 'baz-%s-end' % id(self)
    # collect from an object
    app = App()
    functions = dict(zeronimo.collect_remote_functions(app))
    assert set(functions.keys()) == set(['foo', 'bar', 'baz'])
    # collect from a class
    functions = dict(zeronimo.collect_remote_functions(App))
    assert set(functions.keys()) == set(['foo', 'bar', 'baz'])


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


@green
def test_tunnel(customer, worker):
    start_workers([worker])
    assert len(customer.tunnels) == 0
    with customer.link([worker]) as tunnel:
        assert len(customer.tunnels) == 1
    assert len(customer.tunnels) == 0
    '''
    with customer.link([worker]) as tunnel1, \
         customer.link([worker]) as tunnel2:
        assert not customer.running
        assert len(customer.tunnels) == 2
        tunnel1.add(0, 0)
        assert customer.running
    '''
    assert len(customer.tunnels) == 0
    busywait(lambda: customer.running, timeout=1)
    assert not customer.running


@green
def test_return(customer, worker):
    start_workers([worker])
    with customer.link([worker]) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'
        assert tunnel.add(2, 2) == 'cutie'
        assert tunnel.add(3, 3) == 'cutie'
        assert tunnel.add(4, 4) == 'cutie'
        assert tunnel.add(5, 5) == 'cutie'
        assert tunnel.add(6, 6) == 'xoxoxoxoxoxo cutie'
        assert tunnel.add(42, 12) == 54


@green
def test_yield(customer, worker):
    start_workers([worker])
    with customer.link([worker]) as tunnel:
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


@green
def test_raise(customer, worker):
    start_workers([worker])
    with customer.link([worker]) as tunnel:
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
        rocket_launching = tunnel.launch_rocket()
        assert rocket_launching.next() == 3
        assert rocket_launching.next() == 2
        assert rocket_launching.next() == 1
        with pytest.raises(RuntimeError):
            rocket_launching.next()


@green
def test_2to1(customer1, customer2, worker):
    start_workers([worker])
    def test(tunnel):
        assert tunnel.add(1, 1) == 'cutie'
        assert len(list(tunnel.jabberwocky())) == 4
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
    with customer1.link([worker]) as tunnel1, \
         customer2.link([worker]) as tunnel2:
        joinall([spawn(test, tunnel1), spawn(test, tunnel2)])


@green
def test_1to2(customer, worker1, worker2):
    start_workers([worker1, worker2])
    with customer.link([worker1, worker2], as_task=True) as tunnel:
        task1 = tunnel.add(1, 1)
        task2 = tunnel.add(2, 2)
        assert task1() == 'cutie'
        assert task2() == 'cutie'
        assert task1.worker_addr != task2.worker_addr


@green
def test_fanout(customer, worker1, worker2):
    start_workers([worker1, worker2])
    with customer.link([worker1, worker2]) as tunnel:
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


@green
def test_slow(customer, worker):
    start_workers([worker])
    with customer.link([worker]) as tunnel:
        with pytest.raises(Timeout):
            with Timeout(0.1):
                tunnel.sleep()
        assert tunnel.sleep() == 'slept'


@green
def _test_link_to_addrs(customer, worker):
    start_workers([worker])
    with customer.link(worker.addrs) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'
