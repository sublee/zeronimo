# -*- coding: utf-8 -*-
from collections import namedtuple
from fnmatch import fnmatch
import functools
import inspect
import os
import platform
from types import FunctionType

import zmq.green as zmq

import zeronimo


FEED_DIR = os.path.join(os.path.dirname(__file__), '_feeds')
WINDOWS = platform.system() == 'Windows'


# deferred fixtures


make_deferred_fixture = lambda name: namedtuple(name, ['protocol'])
will_be_worker = make_deferred_fixture('will_be_worker')
will_be_customer = make_deferred_fixture('will_be_customer')
will_be_fanout_customer = make_deferred_fixture('will_be_fanout_customer')
will_be_collector = make_deferred_fixture('will_be_collector')
will_be_addr = make_deferred_fixture('will_be_addr')
will_be_fanout_addr = make_deferred_fixture('will_be_fanout_addr')
will_be_topic = make_deferred_fixture('will_be_topic')
will_be_ctx = make_deferred_fixture('will_be_ctx')
deferred_fixtures = {
    'worker*': will_be_worker,
    'customer*': will_be_customer,
    'fanout_customer*': will_be_fanout_customer,
    'collector*': will_be_collector,
    'addr*': will_be_addr,
    'fanout_addr*': will_be_fanout_addr,
    'topic*': will_be_topic,
    'ctx': will_be_ctx,
}


# addressing


def rand_str(size=6):
    import random
    import string
    return ''.join(random.choice(string.ascii_lowercase) for x in xrange(size))


class AddressGenerator(object):

    @classmethod
    def inproc(cls):
        """Generates random in-process address."""
        return 'inproc://{0}'.format(rand_str())

    @classmethod
    def ipc(cls):
        """Generates available IPC address."""
        if not os.path.isdir(FEED_DIR):
            os.mkdir(FEED_DIR)
        pipe = None
        while pipe is None or os.path.exists(pipe):
            pipe = os.path.join(FEED_DIR, rand_str())
        return 'ipc://{0}'.format(pipe)

    @classmethod
    def tcp(cls):
        """Generates available TCP address."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', 0))
        addr = 'tcp://{0}:{1}'.format(*sock.getsockname())
        sock.close()
        return addr

    @classmethod
    def pgm(cls):
        """Generates available PGM address."""
        return 'pgm://127.0.0.1;224.0.1.0:5555'

    @classmethod
    def epgm(cls):
        """Generates available Encapsulated PGM address."""
        return 'e' + cls.pgm()


gen_addr = lambda protocol: getattr(AddressGenerator, protocol)()


def pytest_addoption(parser):
    parser.addoption('--all', action='store_true',
                     help='tests with all protocols.')
    parser.addoption('--inproc', action='store_true',
                     help='tests with inproc protocol.')
    ipc_help = 'ignored in windows.' if WINDOWS else 'tests with ipc protocol.'
    parser.addoption('--ipc', action='store_true',
                     help=ipc_help)
    parser.addoption('--tcp', action='store_true',
                     help='tests with tcp protocol.')
    parser.addoption('--pgm', action='store_true',
                     help='tests with pgm protocol.')
    parser.addoption('--epgm', action='store_true',
                     help='tests with epgm protocol.')
    parser.addoption('--timeout', action='store', type='float',
                     default=0.01, help='finding timeout in seconds.')
    parser.addoption('--clear', action='store_true',
                     help='destroy context at each tests done.')


def pytest_configure(config):
    globals()['config'] = config
    init_collector = zeronimo.Collector.__init__
    def patched_init_collector(self, *args, **kwargs):
        kwargs.setdefault('timeout', config._adjusted_timeout)
        init_collector(self, *args, **kwargs)
    zeronimo.Collector.__init__ = init_collector


def pytest_unconfigure(config):
    if os.path.isdir(FEED_DIR):
        shutil.rmtree(FEED_DIR)


def pytest_generate_tests(metafunc):
    """Generates worker and customer fixtures."""
    argnames = []
    argvalues = []
    ids = []
    for protocol in get_testing_protocols(metafunc):
        curargvalues = []
        tunnel_socks = []
        for param in metafunc.fixturenames:
            for pattern, deferred_fixture in deferred_fixtures.iteritems():
                if fnmatch(param, pattern):
                    curargvalues.append(deferred_fixture(protocol))
                    break
            else:
                continue
            if not ids:
                argnames.append(param)
        argvalues.append(curargvalues)
        ids.append(protocol)
    if argnames:
        metafunc.parametrize(argnames, argvalues, ids=ids)


def get_testing_protocols(metafunc):
    # windows doesn't support ipc
    if metafunc.config.option.all:
        testing_protocols = ['inproc', 'ipc', 'tcp', 'pgm', 'epgm']
    else:
        testing_protocols = []
        if metafunc.config.getoption('--inproc'):
            testing_protocols.append('inproc')
        if metafunc.config.getoption('--ipc'):
            testing_protocols.append('ipc')
        if metafunc.config.getoption('--tcp'):
            testing_protocols.append('tcp')
        if metafunc.config.getoption('--pgm'):
            testing_protocols.append('pgm')
        if metafunc.config.getoption('--epgm'):
            testing_protocols.append('epgm')
    if WINDOWS:
        try:
            testing_protocols.remove('ipc')
        except ValueError:
            pass
    if not testing_protocols:
        raise RuntimeError('Should specify testing protocols')
    return testing_protocols


def resolve_fixtures(f, protocol):
    @functools.wraps(f)
    def fixture_resolved(**kwargs):
        ctx = zmq.Context()
        resolved_kwargs = {}
        for param, val in kwargs.iteritems():
            if isinstance(val, will_be_worker):
                val = 'Worker'
            elif isinstance(val, will_be_addr):
                val = gen_addr(protocol)
            elif isinstance(val, will_be_topic):
                val = rand_str()
            elif isinstance(val, will_be_ctx):
                val = ctx
            kwargs[param] = val
        return f(**kwargs)
    return fixture_resolved


# decorate functions which use deferred fixtures with resolve_fixtures
# automatically.
import _pytest.python
genfunctions = _pytest.python.PyCollector._genfunctions
def patched_genfunctions(*args, **kwargs):
    for function in genfunctions(*args, **kwargs):
        try:
            callspec = function.callspec
        except AttributeError:
            pass
        else:
            if callspec._idlist:
                protocol = callspec._idlist[0]
                function.obj = resolve_fixtures(function.obj, protocol)
        yield function
_pytest.python.PyCollector._genfunctions = patched_genfunctions


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
    protocol, endpoint = addr.split('://', 1)
    if protocol == 'inproc':
        gevent.sleep(TICK)
        return
    elif protocol == 'ipc':
        still_exists = lambda: os.path.exists(endpoint)
    elif protocol == 'tcp':
        host, port = endpoint.split(':')
        port = int(port)
        def still_exists():
            for conn in ps.get_connections():
                if conn.local_address == (host, port):
                    return True
            return False
    with gevent.Timeout(timeout, '{} still exists'.format(addr)):
        while still_exists():
            gevent.sleep(TICK)


def sync_pubsub(pub_sock, sub_socks, topic=''):
    """A PUB socket needs to receive subscription messages from the SUB sockets
    for establishing cocnnections. It takes very short time but not
    immediately.

    This function synchronizes for a PUB socket can send messages to all the
    SUB sockets.

       >>> sub_sock1.set(zmq.SUBSCRIBE, 'test')
       >>> sub_sock2.set(zmq.SUBSCRIBE, 'test')
       >>> sync_pubsub(pub_sock, [sub_sock1, sub_sock2], topic='test')
    """
    poller = zmq.Poller()
    for sub_sock in sub_socks:
        poller.register(sub_sock, zmq.POLLIN)
    to_sync = sub_socks[:]
    # sync all SUB sockets
    with gevent.Timeout(1, 'Are SUB sockets subscribing?'):
        while to_sync:
            pub_sock.send(topic + ':sync')
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
