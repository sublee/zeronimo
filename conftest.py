# -*- coding: utf-8 -*-
import os
import random
import re
import socket
import sys
import types

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
    parser.addoption('--all', action='store_true', help='use all protocols.')
    parser.addoption('--no-inproc', action='store_true',
                     help='don\'t use inproc protocol.')
    parser.addoption('--no-ipc', action='store_true',
                     help='don\'t use ipc protocol.')
    parser.addoption('--tcp', action='store_true', help='use tcp protocol.')
    parser.addoption('--pgm', action='store_true', help='use pgm protocol.')
    parser.addoption('--epgm', action='store_true', help='use epgm protocol.')


def get_testing_protocols(metafunc):
    if metafunc.config.option.all:
        testing_protocols = ['inproc', 'ipc', 'tcp', 'pgm', 'epgm']
    else:
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
    return testing_protocols


def pytest_generate_tests(metafunc):
    """Generates worker and customer fixtures.

    - worker_info[n] -- a tuple containing the :class:`Worker` object, the
                        address PULL socket bound, and the address SUB socket
                        bound.
    - customer_info[n] -- a tuple containing the :class:`Customer` object, and
                          the address PULL socket bound.
    """
    argnames = []
    argvalues = []
    ids = []
    for protocol in get_testing_protocols(metafunc):
        curargvalues = []
        tunnel_socks = []
        for param in metafunc.fixturenames:
            if param.startswith('worker'):
                # defer making a worker in autowork
                curargvalues.append(('__make_worker', protocol))
            elif param.startswith('customer'):
                # defer making a customer in autowork
                curargvalues.append(('__make_customer', protocol))
            elif re.match('tunnel_sock(et)?s', param):
                # defer making tunnel sockets which connect to the workers
                curargvalues.append(('__make_tunnel_sockets',))
            elif param == 'prefix':
                curargvalues.append(('__prefix',))
            elif param.startswith('addr'):
                curargvalues.append(('__addr', protocol))
            elif param.startswith('fanout_addr'):
                curargvalues.append(('__fanout_addr', protocol))
            else:
                continue
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


protocols = {'inproc': (inproc, inproc), 'ipc': (ipc, ipc), 'tcp': (tcp, tcp),
             'pgm': (tcp, pgm), 'epgm': (tcp, epgm)}


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


def make_worker(protocol, prefix=''):
    """Creates a :class:`zeronimo.Worker` by the given protocol."""
    make_addr, make_fanout_addr = protocols[protocol]
    pull_sock = zmq_context.socket(zmq.PULL)
    pull_addr = make_addr()
    pull_sock.bind(pull_addr)
    sub_sock = zmq_context.socket(zmq.SUB)
    sub_addr = make_fanout_addr()
    sub_sock.bind(sub_addr)
    sub_sock.set(zmq.SUBSCRIBE, prefix)
    worker_info = (pull_addr, sub_addr, prefix)
    worker = zeronimo.Worker(app, [pull_sock, sub_sock], worker_info)
    return worker


def make_customer(protocol):
    """Creates a :class:`zeronimo.Customer` by the given protocol."""
    make_addr, __ = protocols[protocol]
    addr = make_addr()
    sock = zmq_context.socket(zmq.PULL)
    sock.bind(addr)
    customer = zeronimo.Customer(addr, sock)
    return customer


def make_tunnel_sockets(workers):
    prefix = None
    for worker in workers:
        if prefix is None:
            prefix = worker.info[-1]
        elif prefix != worker.info[-1]:
            raise ValueError('All workers must have same subscription')
        if worker.is_running():
            raise RuntimeError('make_tunnel_sockets must be called before '
                               'workers run')
    prefixes = set(worker.info[-1] for worker in workers)
    assert len(prefixes) == 1
    prefix = next(iter(prefixes))
    push = zmq_context.socket(zmq.PUSH)
    pub = zmq_context.socket(zmq.PUB)
    subs = []
    pgm_addrs = set()
    for worker in workers:
        subs.extend(sock for sock in worker.sockets
                    if sock.socket_type == zmq.SUB)
        push_addr, pub_addr, prefix = worker.info
        push.connect(push_addr)
        if re.match('e?pgm://', pub_addr):
            if pub_addr in pgm_addrs:
                continue
            pgm_addrs.add(pub_addr)
        pub.connect(pub_addr)
    sync_pubsub(pub, subs, prefix)
    return (push, pub)


@decorator
def green(f, *args, **kwargs):
    """Runs the function within a greenlet."""
    return gevent.spawn(f, *args, **kwargs).get()


@decorator
def autowork(f, *args):
    """Workers which are yielded by the function will start and stop
    automatically.
    """
    args = list(args)
    # process '__make_worker', '__make_customer', '__prefix'
    prefix = zeronimo.alloc_id()
    workers = []
    for x, arg in enumerate(args):
        if not isinstance(arg, tuple):
            continue
        action = arg[0]
        if action == '__make_worker':
            worker = make_worker(arg[1], prefix)
            workers.append(worker)
            args[x] = worker
        elif action == '__make_customer':
            args[x] = make_customer(arg[1])
        elif action == '__prefix':
            args[x] = prefix
        elif action == '__addr':
            args[x] = protocols[arg[1]][0]()
        elif action == '__fanout_addr':
            args[x] = protocols[arg[1]][1]()
    wills = []
    def reserve(will):
        assert next(will) is None
        wills.append(will)
    if workers:
        # process '__make_tunnel_sockets'
        for x, arg in enumerate(args):
            if not isinstance(arg, tuple):
                continue
            action = arg[0]
            if action == '__make_tunnel_sockets':
                tunnel_socks = make_tunnel_sockets(workers)
                args[x] = tunnel_socks
                [reserve(autowork.will(sock.close)) for sock in tunnel_socks]
    # Worker.start should be called after make_tunnel_sockets
    for arg in args:
        if isinstance(arg, zeronimo.Runner):
            arg.start()
            reserve(autowork.will_stop(arg))
    # run the function
    f = green(f)
    try:
        rv = f(*args)
        if isinstance(rv, types.GeneratorType):
            for will in rv:
                reserve(will)
    finally:
        for will in wills:
            with pytest.raises(StopIteration):
                next(will)
        assert not ps.get_connections()


def will(function, *args, **kwargs):
    yield
    function(*args, **kwargs)


def will_stop(runner):
    yield
    try:
        runner.stop()
    except RuntimeError:
        pass
    if isinstance(runner, zeronimo.Worker):
        for sock in runner.sockets:
            sock.close()
    elif isinstance(runner, zeronimo.Customer):
        runner.socket.close()
    assert not runner.is_running()


autowork.will = will
autowork.will_stop = will_stop


def busywait(func, equal=True):
    """Sleeps while the ``while_`` function returns ``True``."""
    while func() == equal:
        gevent.sleep(0.001)


def test_worker(worker):
    """Checks that the address is connectable."""
    assert 0
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
    for sub_sock in sub_socks:
        poller.register(sub_sock, zmq.POLLIN)
    to_sync = sub_socks[:]
    while to_sync:
        pub_sock.send(prefix + ':sync')
        events = dict(poller.poll(timeout=100))
        for sub_sock in sub_socks:
            if sub_sock in events:
                assert sub_sock.recv().endswith(':sync')
                try:
                    to_sync.remove(sub_sock)
                except ValueError:
                    pass
    while True:
        events = poller.poll(timeout=100)
        if not events:
            break
        for sub_sock, event in events:
            assert sub_sock.recv().endswith(':sync')


def sync_fanout(tunnel, workers):
    tunnel_sock = tunnel._znm_sockets[zmq.PUB]
    worker_socks = []
    for worker in workers:
        worker_socks.extend([sock for sock in worker.sockets
                             if sock.socket_type == zmq.SUB])
    sync_pubsub(tunnel_sock, worker_socks, tunnel._znm_prefix)
