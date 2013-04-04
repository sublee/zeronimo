import functools
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
#gevent.hub.get_hub().print_exception = lambda *a, **k: 'do not print exception'


@decorator
def green(f, *args, **kwargs):
    print
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


def start_workers(workers):
    waits = []
    for worker in workers:
        spawn(worker.run)
        until = lambda: is_connectable(worker.addrs[0], zmq.PUSH)
        waits.append(spawn(busywait, until, until=True, timeout=1))
    joinall(waits)


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
    def zero_div(self):
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

    @zeronimo.register
    def rycbar123(self):
        for word in 'run, you clever boy; and remember.'.split():
            yield word

    @zeronimo.register
    def sleep(self):
        gevent.sleep(0.1)
        return 'slept'


# fixtures


for x in xrange(4):
    exec(textwrap.dedent('''
    @pytest.fixture
    def worker{x}():
        app = Application()
        return zeronimo.Worker(app, context=zmq_context)
    @pytest.fixture
    def customer{x}():
        return zeronimo.Customer(context=zmq_context)
    ''').format(x=x if x else ''))


# tests


def _test_blueprint_extraction():
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
    blueprint = dict(zeronimo.functional.extract_blueprint(app))
    assert not blueprint['foo'].fanout
    assert blueprint['foo'].reply
    assert blueprint['foo'].reply
    assert blueprint['bar'].fanout
    assert blueprint['bar'].reply
    assert not blueprint['baz'].fanout
    assert blueprint['baz'].reply
    # collect from a class
    blueprint = dict(zeronimo.functional.extract_blueprint(App))
    assert not blueprint['foo'].fanout
    assert blueprint['foo'].reply
    assert blueprint['bar'].fanout
    assert blueprint['bar'].reply
    assert not blueprint['baz'].fanout
    assert blueprint['baz'].reply


def _test_fingerprint():
    class Nothing(object): pass
    blueprint = dict(zeronimo.functional.extract_blueprint(Application))
    blueprint2 = dict(zeronimo.functional.extract_blueprint(Application()))
    blueprint3 = dict(zeronimo.functional.extract_blueprint(Nothing))
    fingerprint = zeronimo.functional.make_fingerprint(blueprint)
    fingerprint2 = zeronimo.functional.make_fingerprint(blueprint2)
    fingerprint3 = zeronimo.functional.make_fingerprint(blueprint3)
    assert fingerprint == fingerprint2
    assert fingerprint != fingerprint3


def test_default_addr(customer, worker):
    assert worker.addrs[0].startswith('inproc://')
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
def test_tunnel(customer, worker):
    start_workers([worker])
    assert len(customer.tunnels) == 0
    with customer.link([worker]) as tunnel:
        assert len(customer.tunnels) == 1
    assert len(customer.tunnels) == 0
    with customer.link([worker]) as tunnel1, \
         customer.link([worker]) as tunnel2:
        assert not customer.running
        assert len(customer.tunnels) == 2
        tunnel1.add(0, 0)
        assert customer.running
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
        failures = tunnel(as_task=True, fanout=True).zero_div()
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
