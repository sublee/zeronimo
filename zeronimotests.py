import functools
from itertools import chain
import os
import socket
import sys
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
    return spawn(f, *args, **kwargs).get()


@decorator
def autowork(f, *args, **kwargs):
    all_workers = []
    try:
        for workers in f(*args, **kwargs):
            all_workers.extend(workers)
            start_workers(workers)
    finally:
        stop_workers(all_workers)


def busywait(func, equal=True):
    """Sleeps while the ``while_`` function returns ``True``."""
    while func() == equal:
        gevent.sleep(0.001)


def is_connectable(worker):
    """Checks that the address is connectable."""
    worker.obj._znm_test = lambda: True
    try:
        customer = zeronimo.Customer(tcp(), context=worker.context)
        tunnel = customer.link_workers([worker])
        tunnel.__enter__()
    except zmq.ZMQError, e:
        if str(e) == 'Connection refused':
            return False
        raise
    else:
        try:
            tunnel._znm_test()
        except zeronimo.AcceptanceError:
            return False
        else:
            return True
    finally:
        try:
            del worker.obj._znm_test
        except AttributeError:
            pass
        tunnel.__exit__(*sys.exc_info())
        customer._running_lock.wait()


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def tcp():
    return 'tcp://127.0.0.1:{0}'.format(free_port())


def inproc():
    return 'inproc://{0}'.format(zeronimo.uuid_str())


def wait_workers(workers, for_binding):
    waits = []
    for worker in workers:
        check = lambda: is_connectable(worker)
        waits.append(spawn(busywait, check, equal=not for_binding))
    with Timeout(1):
        joinall(waits)


def start_workers(workers):
    for worker in workers:
        spawn(worker.run)
    wait_workers(workers, for_binding=True)


def stop_workers(workers):
    for worker in workers:
        worker.stop()
    wait_workers(workers, for_binding=False)


class Application(object):

    def simple(self):
        return 'ok'

    def add(self, a, b):
        """Koreans' mathematical addition."""
        if a == b:
            if 1 <= a < 6:
                return 'cutie'
            elif a == 6:
                return 'xoxoxoxoxoxo cutie'
        return a + b

    def jabberwocky(self):
        yield 'Twas brillig, and the slithy toves'
        yield 'Did gyre and gimble in the wabe;'
        yield 'All mimsy were the borogoves,'
        yield 'And the mome raths outgrabe.'

    def xrange(self):
        return xrange(5)

    def dict_view(self):
        return dict(zip(xrange(5), xrange(5))).viewkeys()

    def dont_yield(self):
        if False:
            yield 'it should\'t be sent'
            assert 0

    def zero_div(self):
        0/0      /0/0 /0/0    /0/0   /0/0/0/0   /0/0
        0/0  /0  /0/0 /0/0    /0/0 /0/0    /0/0     /0
        0/0/0/0/0/0/0 /0/0/0/0/0/0 /0/0    /0/0   /0
        0/0/0/0/0/0/0 /0/0    /0/0 /0/0    /0/0
        0/0  /0  /0/0 /0/0    /0/0   /0/0/0/0     /0

    def launch_rocket(self):
        yield 3
        yield 2
        yield 1
        raise RuntimeError('Launch!')

    def rycbar123(self):
        for word in 'run, you clever boy; and remember.'.split():
            yield word

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


@green
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


@green
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


@green
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


@green
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


@green
@autowork
def test_1to2(customer, worker1, worker2):
    yield [worker1, worker2]
    with customer.link_workers([worker1, worker2], as_task=True) as tunnel:
        task1 = tunnel.add(1, 1)
        task2 = tunnel.add(2, 2)
        assert task1() == 'cutie'
        assert task2() == 'cutie'
        assert task1.worker_addr != task2.worker_addr


@green
@autowork
def test_fanout(customer, worker1, worker2):
    yield [worker1, worker2]
    with customer.link_workers([worker1, worker2]) as tunnel:
        assert list(tunnel.rycbar123()) == \
               'run, you clever boy; and remember.'.split()
        print tunnel(fanout=True)._znm_fanout
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
@autowork
def test_slow(customer, worker):
    yield [worker]
    with customer.link_workers([worker]) as tunnel:
        with pytest.raises(Timeout):
            with Timeout(0.1):
                tunnel.sleep()
        assert tunnel.sleep() == 'slept'


@green
@autowork
def test_link_to_addrs(customer, worker):
    yield [worker]
    with customer.link(worker.addrs, worker.fanout_addrs) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'


@green
@autowork
def test_reject(customer, worker1, worker2):
    yield [worker1, worker2]
    with customer.link_workers([worker1, worker2]) as tunnel:
        assert len(list(tunnel(fanout=True).simple())) == 2
        worker2.reject_all()
        assert len(list(tunnel(fanout=True).simple())) == 1
        worker2.accept_all()
        assert len(list(tunnel(fanout=True).simple())) == 2


@green
@autowork
def test_topic(customer, worker1, worker2):
    yield [worker1, worker2]
    fanout_addrs = list(worker1.fanout_addrs) + list(worker2.fanout_addrs)
    with customer.link(fanout_addrs=fanout_addrs) as tunnel:
        assert len(list(tunnel(fanout=True).simple())) == 2
        worker2.subscribe('stop')
        assert len(list(tunnel(fanout=True).simple())) == 1
        worker1.subscribe('stop')
        with pytest.raises(zeronimo.AcceptanceError):
            tunnel(fanout=True).simple()
        worker1.subscribe('')
        assert len(list(tunnel(fanout=True).simple())) == 1
        worker2.subscribe('')
        assert len(list(tunnel(fanout=True).simple())) == 2


@green
@autowork
def test_ipc():
    app = Application()
    worker1 = zeronimo.Worker(app)
    worker2 = zeronimo.Worker(app)
    customer1 = zeronimo.Customer()
    customer2 = zeronimo.Customer()
    # bind to ipc
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
    assert not os.path.exists('_feeds/customer1')
    assert not os.path.exists('_feeds/customer2')


@green
@autowork
def test_tcp():
    app = Application()
    worker1 = zeronimo.Worker(app)
    worker2 = zeronimo.Worker(app)
    customer1 = zeronimo.Customer()
    customer2 = zeronimo.Customer()
    # bind to tcp
    ports = [free_port() for x in xrange(6)]
    tcp = lambda x: 'tcp://*:{}'.format(ports[x])
    worker1.bind(tcp(0))
    worker1.bind_fanout(tcp(1))
    worker2.bind(tcp(2))
    worker2.bind_fanout(tcp(3))
    customer1.bind(tcp(4))
    customer2.bind(tcp(5))
    yield [worker1, worker2]
    with customer1.link_workers([worker1]) as tunnel1:
        assert tunnel1.simple() == 'ok'
    with customer1.link_workers([worker1, worker2]) as tunnel1, \
         customer2.link_workers([worker1, worker2]) as tunnel2:
        assert tunnel1.simple() == 'ok'
        assert tunnel2.simple() == 'ok'
        assert list(tunnel1(fanout=True).simple()) == ['ok', 'ok']
