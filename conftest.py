# -*- coding: utf-8 -*-
from collections import namedtuple
import functools
import itertools
import os
import platform
import random
import re
import shutil
import socket
import sys
import types

from decorator import decorator
import gevent
import psutil
import pytest
import zmq.green as zmq

import zeronimo


TICK = 0.001
FEED_DIR = os.path.join(os.path.dirname(__file__), '_feeds')


ps = psutil.Process(os.getpid())
windows = platform.system() == 'Windows'
#gevent.hub.get_hub().print_exception = lambda *a, **k: 'do not print exception'


def pytest_addoption(parser):
    parser.addoption('--all', action='store_true',
                     help='tests with all protocols.')
    parser.addoption('--inproc', action='store_true',
                     help='tests with inproc protocol.')
    ipc_help = 'ignored in windows.' if windows else 'tests with ipc protocol.'
    parser.addoption('--ipc', action='store_true',
                     help=ipc_help)
    parser.addoption('--tcp', action='store_true',
                     help='tests with tcp protocol.')
    parser.addoption('--pgm', action='store_true',
                     help='tests with pgm protocol.')
    parser.addoption('--epgm', action='store_true',
                     help='tests with epgm protocol.')


def get_testing_protocols(metafunc):
    # windows doesn't support ipc
    if metafunc.config.option.all:
        testing_protocols = ['inproc', 'ipc', 'tcp', 'pgm', 'epgm']
    else:
        testing_protocols = []
        if metafunc.config.option.inproc:
            testing_protocols.append('inproc')
        if metafunc.config.option.ipc:
            testing_protocols.append('ipc')
        if metafunc.config.option.tcp:
            testing_protocols.append('tcp')
        if metafunc.config.option.pgm:
            testing_protocols.append('pgm')
        if metafunc.config.option.epgm:
            testing_protocols.append('epgm')
    if windows:
        try:
            testing_protocols.remove('ipc')
        except ValueError:
            pass
    if not testing_protocols:
        raise RuntimeError('Should specify testing protocols')
    return testing_protocols


def inproc():
    """Generates random in-process address."""
    return 'inproc://{0}'.format(zeronimo.alloc_id())


def ipc():
    """Generates available IPC address."""
    if not os.path.isdir(FEED_DIR):
        os.mkdir(FEED_DIR)
    pipe = None
    while pipe is None or os.path.exists(pipe):
        pipe = os.path.join(FEED_DIR, zeronimo.alloc_id())
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


protocols = {'inproc': (inproc, inproc),
             'ipc': (ipc, ipc),
             'tcp': (tcp, tcp),
             'pgm': (tcp, pgm),  # addresses for direct and fanout
             'epgm': (tcp, epgm)}


class Application(object):
    """The sample application."""

    def simple(self):
        return 'ok'

    def add(self, a, b):
        """a + b."""
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


def make_worker(ctx, protocol, prefix=''):
    """Creates a :class:`zeronimo.Worker` by the given protocol."""
    make_addr, make_fanout_addr = protocols[protocol]
    pull_sock = ctx.socket(zmq.PULL)
    pull_addr = make_addr()
    pull_sock.bind(pull_addr)
    sub_sock = ctx.socket(zmq.SUB)
    sub_addr = make_fanout_addr()
    sub_sock.bind(sub_addr)
    sub_sock.set(zmq.SUBSCRIBE, prefix)
    worker_info = (pull_addr, sub_addr, prefix)
    worker = zeronimo.Worker(app, [pull_sock, sub_sock], worker_info)
    return worker


def make_customer(ctx, protocol):
    """Creates a :class:`zeronimo.Customer` by the given protocol."""
    make_addr, __ = protocols[protocol]
    addr = make_addr()
    sock = ctx.socket(zmq.PULL)
    sock.bind(addr)
    customer = zeronimo.Customer(sock, addr)
    return customer


def make_tunnel_sockets(ctx, workers):
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
    push = ctx.socket(zmq.PUSH)
    pub = ctx.socket(zmq.PUB)
    subs = []
    #pgm_addrs = set()
    for worker in workers:
        subs.extend(sock for sock in worker.sockets
                    if sock.socket_type == zmq.SUB)
        push_addr, pub_addr, prefix = worker.info
        push.connect(push_addr)
        #if re.match('e?pgm://', pub_addr):
        #    if pub_addr in pgm_addrs:
        #        continue
        #    pgm_addrs.add(pub_addr)
        pub.connect(pub_addr)
    sync_pubsub(pub, subs, prefix)
    return {push: push_addr, pub: pub_addr}


def patch_worker_to_be_slow(worker, delay):
    def run_task(self, invocation, context):
        self._slow = invocation.function_name != '_znm_test'
        return zeronimo.Worker.run_task(self, invocation, context)
    def send_reply(self, sock, method, *args, **kwargs):
        if self._slow and method == zeronimo.ACCEPT:
            gevent.sleep(delay)
        return zeronimo.Worker.send_reply(self, sock, method, *args, **kwargs)
    worker.run_task = functools.partial(run_task, worker)
    worker.send_reply = functools.partial(send_reply, worker)


# zmq helpers


def link_sockets(addr, server_sock, client_socks):
    while True:
        try:
            server_sock.bind(addr)
        except zmq.ZMQError:
            gevent.sleep(TICK)
        else:
            break
    for sock in client_socks:
        sock.connect(addr)


def wait_to_close(addr, timeout=1):
    protocol, location = addr.split('://', 1)
    if protocol == 'inproc':
        gevent.sleep(TICK)
        return
    elif protocol == 'ipc':
        still_exists = lambda: os.path.exists(location)
    elif protocol == 'tcp':
        host, port = location.split(':')
        port = int(port)
        def still_exists():
            for conn in ps.get_connections():
                if conn.local_address == (host, port):
                    print 'still exists'
                    return True
            return False
    with gevent.Timeout(timeout, '{} still exists'.format(addr)):
        while still_exists():
            gevent.sleep(TICK)


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
    # sync all SUB sockets
    while to_sync:
        pub_sock.send(prefix + ':sync')
        events = dict(poller.poll(timeout=1))
        for sub_sock in sub_socks:
            if sub_sock in events:
                assert sub_sock.recv().endswith(':sync')
                try:
                    to_sync.remove(sub_sock)
                except ValueError:
                    pass
    # discard garbage sync messges
    while True:
        events = poller.poll(timeout=1)
        if not events:
            break
        for sub_sock, event in events:
            assert sub_sock.recv().endswith(':sync')


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


@decorator
def green(f, *args, **kwargs):
    """Runs the function within a greenlet."""
    return gevent.spawn(f, *args, **kwargs).get()


deferred_worker = namedtuple('deferred_worker', ['protocol'])
deferred_customer = namedtuple('deferred_cutomer', ['protocol'])
deferred_addr = namedtuple('deferred_addr', ['protocol'])
deferred_fanout_addr = namedtuple('deferred_fanout_addr', ['protocol'])
deferred_tunnel_sockets = namedtuple('deferred_tunnel_sockets', [])
deferred_prefix = namedtuple('deferred_prefix', [])
deferred_ctx = namedtuple('deferred_ctx', [])


def is_unexpected_conn(conn):
    ports = [addr[1] if addr else None
             for addr in (conn.local_address, conn.remote_address)]
    if windows and 5905 in ports:
        # libzmq uses TCP port 5905 for the signaler in Windows.
        return False
    return conn.status in ('LISTEN', 'ESTABLISHED')


@decorator
def autowork(f, *args):
    """Workers which are yielded by the function will start and stop
    automatically.
    """
    args = list(args)
    ctx = zmq.Context()
    # process deferred worker, customer, prefix, addr, fanout_addr
    prefix = zeronimo.alloc_id()
    protocol = None
    workers = []
    for x, arg in enumerate(args):
        if protocol is None and hasattr(arg, 'protocol'):
            protocol = arg.protocol
        if isinstance(arg, deferred_worker):
            worker = make_worker(ctx, arg.protocol, prefix)
            workers.append(worker)
            args[x] = worker
        elif isinstance(arg, deferred_customer):
            args[x] = make_customer(ctx, arg.protocol)
        elif isinstance(arg, deferred_addr):
            args[x] = protocols[arg.protocol][0]()
        elif isinstance(arg, deferred_fanout_addr):
            args[x] = protocols[arg.protocol][1]()
        elif isinstance(arg, deferred_prefix):
            args[x] = prefix
        elif isinstance(arg, deferred_ctx):
            args[x] = ctx
    wills = []
    # process deferred tunnel sockets because it should be called before the
    # workers start
    if workers:
        for x, arg in enumerate(args):
            if isinstance(arg, deferred_tunnel_sockets):
                tunnel_socks = make_tunnel_sockets(ctx, workers)
                args[x] = tunnel_socks.keys()
                #for sock, addr in tunnel_socks.iteritems():
                #    wills.append(sock.close)
    # start all runners
    runners = []
    for arg in args:
        if isinstance(arg, zeronimo.Runner):
            arg.start()
            runners.append(arg)
    wills.append(lambda: stop_zeronimo(runners))
    # run the function
    try:
        return green(f)(*args)
    finally:
        for will in wills:
            will()
        #print 'destroy'
        ctx.destroy()
        if protocol == 'ipc':
            shutil.rmtree(FEED_DIR)
        conns = filter(is_unexpected_conn, ps.get_connections())

        assert not conns


def stop_zeronimo(runners):
    def ignore_runtimeerror(f):
        try:
            return f()
        except RuntimeError:
            pass
    stoppings = []
    for runner in runners:
        stoppings.append(gevent.spawn(ignore_runtimeerror, runner.stop))
    gevent.joinall(stoppings, raise_error=True)


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
                curargvalues.append(deferred_worker(protocol))
            elif param.startswith('customer'):
                # defer making a customer in autowork
                curargvalues.append(deferred_customer(protocol))
            elif re.match('tunnel_sock(et)?s', param):
                # defer making tunnel sockets which connect to the workers
                curargvalues.append(deferred_tunnel_sockets())
            elif param.startswith('addr'):
                curargvalues.append(deferred_addr(protocol))
            elif param.startswith('fanout_addr'):
                curargvalues.append(deferred_fanout_addr(protocol))
            elif param == 'prefix':
                curargvalues.append(deferred_prefix())
            elif param == 'ctx':
                curargvalues.append(deferred_ctx())
            else:
                continue
            if not ids:
                argnames.append(param)
        argvalues.append(curargvalues)
        ids.append(protocol)
    if argnames:
        metafunc.parametrize(argnames, argvalues, ids=ids)
