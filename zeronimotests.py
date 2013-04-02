import functools
import os
import uuid

from decorator import decorator
import gevent
from gevent import joinall, killall, spawn
import pytest
from pytest import raises
import zmq.green as zmq

import zeronimo


zmq_context = zmq.Context()


@decorator
def green(f, *args, **kwargs):
    return spawn(f, *args, **kwargs).get()


def busywait(while_, until=False, timeout=None):
    """Sleeps while the ``while_`` function returns ``True``."""
    until = bool(until)
    with gevent.Timeout(timeout):
        while until != bool(while_()):
            gevent.sleep(0.001)


def zmq_bound(addr, socket_type, context=zmq_context):
    """Checks that the address is connectable."""
    try:
        context.socket(socket_type).connect(addr)
    except zmq.ZMQError:
        return False
    else:
        return True


def ensure_worker(worker):
    spawn(worker.run)
    busywait(lambda: zmq_bound(worker.addr, zmq.PUSH), until=True)


'''
FD_DIR = os.path.join(os.path.dirname(__file__), '_fds')
def generate_endpoint(protocol, name=None, offset=None):
    if protocol in 'inproc':
        if name is None:
            name = str(uuid.uuid4())
        endpoint = 'inproc://{}'.format(name)
        if offset is not None:
            endpoint = '-'.join([endpoint, str(offset)])
        return endpoint
    elif protocol == 'ipc':
        if not os.isdir(FD_DIR):
            os.makedir(FD_DIR)
        fd = int(sorted(os.listdir(FD_DIR), reverse=True)[0]) + 1
        return 'ipc://_fds/{}'.format(fd)
    elif protocol == 'tcp':
        return 'tcp://*:*'
'''


class Application(object):

    @zeronimo.register
    def add(self, a, b):
        """Koreans' mathematical addition."""
        if a == b:
            if 1 <= a < 6:
                return 'cutie'
            elif a == 6:
                return 'xoxoxoxoxoxo cutie'
        return a + b

    @zeronimo.register
    def jabberwocky(self):
        yield 'Twas brillig, and the slithy toves'
        yield 'Did gyre and gimble in the wabe;'
        yield 'All mimsy were the borogoves,'
        yield 'And the mome raths outgrabe.'

    @zeronimo.register
    def xrange(self):
        return xrange(5)

    @zeronimo.register
    def dict_view(self):
        return dict(zip(xrange(5), xrange(5))).viewkeys()

    @zeronimo.register
    def dont_yield(self):
        if False:
            yield 'it should\'t be sent'
            assert 0

    @zeronimo.register
    def divide_by_zero(self):
        0/0      /0/0 /0/0    /0/0   /0/0/0/0   /0/0
        0/0  /0  /0/0 /0/0    /0/0 /0/0    /0/0     /0
        0/0/0/0/0/0/0 /0/0/0/0/0/0 /0/0    /0/0   /0
        0/0/0/0/0/0/0 /0/0    /0/0 /0/0    /0/0
        0/0  /0  /0/0 /0/0    /0/0   /0/0/0/0     /0

    @zeronimo.register
    def launch_rocket(self):
        yield 3
        yield 2
        yield 1
        raise RuntimeError('Launch!')

    @zeronimo.register(fanout=True)
    def rycbar123(self):
        for word in 'Run, you clever boy; and remember.'.split():
            yield word


@pytest.fixture
def worker():
    app = Application()
    return zeronimo.Worker(app, context=zmq_context)


@pytest.fixture
def customer():
    return zeronimo.Customer(context=zmq_context)


def test_remote_method_collection():
    class App(object):
        @zeronimo.register
        def foo(self):
            return 'foo-%s' % id(self)
        @zeronimo.register(fanout=True)
        def bar(self):
            return 'bar-%s' % id(self)
        @zeronimo.register
        def baz(self):
            yield 'baz-%s-begin' % id(self)
            yield 'baz-%s-end' % id(self)
    # collect from an object
    app = App()
    plans = dict(zeronimo.collect_remote_functions(app))
    assert not plans[app.foo].fanout
    assert plans[app.foo].reply
    assert plans[app.bar].fanout
    assert plans[app.bar].reply
    assert not plans[app.baz].fanout
    assert plans[app.baz].reply
    # collect from a class
    plans = dict(zeronimo.collect_remote_functions(App))
    assert not plans[App.foo].fanout
    assert plans[App.foo].reply
    assert plans[App.bar].fanout
    assert plans[App.bar].reply
    assert not plans[App.baz].fanout
    assert plans[App.baz].reply


def test_default_addr(worker, customer):
    assert worker.addr.startswith('inproc://')
    assert customer.addr.startswith('inproc://')


def test_running():
    from zeronimo.core import Communicator
    class TestingCommunicator(Communicator):
        def run(self):
            assert self.running
    comm = TestingCommunicator()
    assert not comm.running
    comm.run()
    assert not comm.running


@green
def test_tunnel(worker, customer):
    ensure_worker(worker)
    assert len(customer.tunnels) == 0
    with customer.link(worker) as tunnel:
        assert len(customer.tunnels) == 1
    assert len(customer.tunnels) == 0
    with customer.link(worker) as tunnel1, customer.link(worker) as tunnel2:
        assert len(customer.tunnels) == 2
    assert len(customer.tunnels) == 0


@green
def test_direct_returning_worker(worker, customer):
    ensure_worker(worker)
    with customer.link(worker) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'
        assert tunnel.add(2, 2) == 'cutie'
        assert tunnel.add(3, 3) == 'cutie'
        assert tunnel.add(4, 4) == 'cutie'
        assert tunnel.add(5, 5) == 'cutie'
        assert tunnel.add(6, 6) == 'xoxoxoxoxoxo cutie'
        assert tunnel.add(42, 12) == 54


@green
def test_direct_yielding_worker(worker, customer):
    ensure_worker(worker)
    with customer.link(worker) as tunnel:
        assert len(list(tunnel.jabberwocky())) == 4
        assert list(tunnel.xrange()) == [0, 1, 2, 3, 4]
        assert list(tunnel.dict_view()) == [0, 1, 2, 3, 4]
        assert list(tunnel.dont_yield()) == []


@green
def test_direct_raising_worker(worker, customer):
    ensure_worker(worker)
    with customer.link(worker) as tunnel:
        with raises(ZeroDivisionError):
            tunnel.divide_by_zero()
        rocket_launching = tunnel.launch_rocket()
        assert rocket_launching.next() == 3
        assert rocket_launching.next() == 2
        assert rocket_launching.next() == 1
        with raises(RuntimeError):
            rocket_launching.next()
