# -*- coding: utf-8 -*-
import os
import random
import socket
import sys

from decorator import decorator
import gevent
import pytest
import zmq.green as zmq

import zeronimo


zmq_context = zmq.Context()
#gevent.hub.get_hub().print_exception = lambda *a, **k: 'do not print exception'


import psutil
ps = psutil.Process(os.getpid())


def pytest_addoption(parser):
    parser.addoption('--no-inproc', action='store_true',
                     help='don\'t use inproc sockets.')
    parser.addoption('--no-ipc', action='store_true',
                     help='don\'t use ipc sockets.')
    parser.addoption('--tcp', action='store_true', help='use tcp sockets.')
    parser.addoption('--pgm', action='store_true', help='use pgm sockets.')
    parser.addoption('--epgm', action='store_true', help='use epgm sockets.')


def pytest_generate_tests(metafunc):
    """Generates worker and customer fixtures."""
    argnames = []
    argvalues = []
    ids = []
    prefix = zeronimo.alloc_id()
    testing_protocols = []
    if not metafunc.config.option.no_inproc:
        testing_protocols.append('inproc')
    if not metafunc.config.option.no_ipc:
        testing_protocols.append('ipc')
    if metafunc.config.option.tcp:
        testing_protocols.append('tcp')
    if metafunc.config.option.pgm:
        testing_protocols.append('pgm')
    if metafunc.config.option.epgm:
        testing_protocols.append('epgm')
    for protocol in testing_protocols:
        curargvalues = []
        for param in metafunc.fixturenames:
            if param.startswith('worker') or param.startswith('customer'):
                if param.startswith('worker'):
                    curargvalues.append(make_worker(protocol, prefix))
                else:
                    curargvalues.append(make_customer(protocol))
                if not ids:
                    argnames.append(param)
        argvalues.append(curargvalues)
        ids.append(protocol)
    if argnames:
        metafunc.parametrize(argnames, argvalues, ids=ids)


def inproc():
    """Generates random in-process address."""
    return 'inproc://{0}'.format(zeronimo.alloc_id())


def ipc():
    """Generates available IPC address."""
    feed_dir = os.path.join(os.path.dirname(__file__), '_feeds')
    if not os.path.isdir(feed_dir):
        os.mkdir(feed_dir)
    pipe = None
    while pipe is None or os.path.exists(pipe):
        pipe = os.path.join(feed_dir, zeronimo.alloc_id())
    return 'ipc://{0}'.format(pipe)


def tcp():
    """Generates available TCP address."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    addr = 'tcp://{0}:{1}'.format(*sock.getsockname())
    sock.close()
    return addr


def pgm():
    """Generates available PGM address."""
    return 'pgm://127.0.0.1;224.1.1.1:5555'


def epgm():
    """Generates available Encapsulated PGM address."""
    return 'epgm://127.0.0.1;224.1.1.1:5555'


protocols = {
    'inproc': (inproc, inproc, zmq_context),
    'ipc': (ipc, ipc, None),
    'tcp': (tcp, tcp, None),
    'pgm': (tcp, pgm, None),
    'epgm': (tcp, epgm, None),
}


class Application(object):
    """The sample application."""

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

    def sleep_range(self, sleep, start, stop=None, step=1):
        if stop is None:
            start, stop = 0, start
        sequence = range(start, stop, step)
        for x, val in enumerate(sequence):
            yield val
            if x < len(sequence) - 1:
                gevent.sleep(sleep)


app = Application()


def make_worker(protocol, prefix):
    """Creates a :class:`zeronimo.Worker` by the given protocol."""
    make_addr, make_fanout_addr, context = protocols[protocol]
    return zeronimo.Worker(
        app, bind=make_addr(), bind_fanout=make_fanout_addr(),
        prefix=prefix, context=context)


def make_customer(protocol):
    """Creates a :class:`zeronimo.Customer` by the given protocol."""
    make_addr, __, context = protocols[protocol]
    return zeronimo.Customer(bind=make_addr(), context=context)


@decorator
def green(f, *args, **kwargs):
    """Runs the function within a greenlet."""
    return gevent.spawn(f, *args, **kwargs).get()


@decorator
def autowork(f, *args, **kwargs):
    """Workers which are yielded by the function will start and stop
    automatically.
    """
    f = green(f)
    workers = []
    greenlets = []
    while True:
        try:
            for objs in f(*args, **kwargs):
                types = list(set(map(type, objs)))
                assert len(types) == 1
                t = types[0]
                if issubclass(t, zeronimo.Worker):
                    workers.extend(objs)
                    start_workers(objs)
                elif issubclass(t, gevent.Greenlet):
                    greenlets.extend(objs)
        except zmq.ZMQError as error:
            if error.errno == 98:
                continue
            raise
        finally:
            stop_workers(workers)
            gevent.killall(greenlets)
        break
    assert not ps.get_connections()


def busywait(func, equal=True):
    """Sleeps while the ``while_`` function returns ``True``."""
    while func() == equal:
        gevent.sleep(0.001)


def test_worker(worker):
    """Checks that the address is connectable."""
    assert hasattr(worker.obj, '_znm_test')
    try:
        customer = zeronimo.Customer(tcp(), context=worker.context)
        tunnel = customer.link_workers([worker])
        tunnel.__enter__()
    except zmq.ZMQError, e:
        if e.errno == zmq.ECONNREFUSED:
            return False
        else:
            raise
    else:
        try:
            tunnel._znm_test()
        except zeronimo.ZeronimoError:
            return False
        try:
            tunnel(fanout=True)._znm_test()
        except zeronimo.ZeronimoError:
            return False
        return True
    finally:
        tunnel.__exit__(*sys.exc_info())
        customer.running_lock.wait()


def wait_workers(workers, for_binding):
    waits = []
    for worker in workers:
        worker.obj._znm_test = lambda: True
        check = lambda: test_worker(worker)
        waits.append(gevent.spawn(busywait, check, equal=not for_binding))
    gevent.joinall(waits)


def start_workers(workers):
    for worker in workers:
        gevent.spawn(worker.run)
    #wait_workers(workers, for_binding=True)
    gevent.sleep(0.5)


def stop_workers(workers):
    for worker in workers:
        try:
            worker.stop()
        except RuntimeError:
            pass
    #wait_workers(workers, for_binding=False)


def run_device(in_sock, out_sock, in_addr=None, out_addr=None):
    try:
        if in_addr is not None:
            in_sock.bind(in_addr)
        if out_addr is not None:
            out_sock.bind(out_addr)
        zmq.device(0, in_sock, out_sock)
    finally:
        in_sock.close()
        out_sock.close()


def sync_pubsub(pub_sock, sub_socks, prefix=''):
    """A PUB socket needs to receive subscription messages from the SUB sockets
    for establishing cocnnections. It takes very short time but not
    immediately.

    This function synchronizes for a PUB socket can send messages to all the
    SUB sockets.

       >>> sub_sock1.set(zmq.SUBSCRIBE, 'test')
       >>> sub_sock2.set(zmq.SUBSCRIBE, 'test')
       >>> sync_pubsub(pub_sock, [sub_sock1, sub_sock2], prefix='test')
    """
    poller = zmq.Poller()
    sub_socks = set(sub_socks)
    for sub_sock in sub_socks:
        poller.register(sub_sock, zmq.POLLIN)
    synced = set()
    while synced != sub_socks:
        pub_sock.send(prefix + ':sync')
        events = dict(poller.poll(timeout=100))
        for sub_sock in sub_socks:
            if sub_sock in events:
                assert sub_sock.recv().endswith(':sync')
                synced.add(sub_sock)
