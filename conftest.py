# -*- coding: utf-8 -*-
from collections import defaultdict, namedtuple
from contextlib import contextmanager
from fnmatch import fnmatch
import functools
import os
import platform
import random
import shutil
import string
import sys
import warnings

import gevent
from gevent import socket
import zmq.green as zmq

import zeronimo


TICK = 0.001
FEED_DIR = os.path.join(os.path.dirname(__file__), '_feeds')
WINDOWS = platform.system() == 'Windows'


config = NotImplemented


# deferred fixtures


make_deferred_fixture = lambda name: namedtuple(name, ['protocol'])
deferred_worker = make_deferred_fixture('deferred_worker')
deferred_collector = make_deferred_fixture('deferred_collector')
deferred_push_socket = make_deferred_fixture('deferred_push_socket')
deferred_pub_socket = make_deferred_fixture('deferred_pub_socket')
deferred_address = make_deferred_fixture('deferred_address')
deferred_fanout_address = make_deferred_fixture('deferred_fanout_address')
deferred_topic = make_deferred_fixture('deferred_topic')
deferred_context = make_deferred_fixture('deferred_context')
deferred_fixtures = {
    'worker*': deferred_worker,
    'collector*': deferred_collector,
    'push*': deferred_push_socket,
    'pub*': deferred_pub_socket,
    'addr*': deferred_address,
    'fanout_addr*': deferred_fanout_address,
    'topic': deferred_topic,
    'ctx': deferred_context,
}


# addressing


def rand_str(size=6):
    return ''.join(random.choice(string.ascii_lowercase) for x in xrange(size))


class AddressGenerator(object):

    @classmethod
    def inproc(cls):
        """Generates random in-process address."""
        return 'inproc://{0}'.format(rand_str())

    @classmethod
    def ipc(cls):
        """Generates available IPC address."""
        try:
            os.mkdir(FEED_DIR)
        except OSError:
            pass
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


def gen_address(protocol, fanout=False):
    if not fanout and protocol.endswith('pgm'):
        protocol = 'tcp'
    return getattr(AddressGenerator, protocol)()


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
    parser.addoption('--patience', action='store', type=float, default=1.,
                     help='multiplier for default emitter timeout.')
    parser.addoption('--incremental-patience', action='store_true',
                     help='increase --patience when a test failed up to 5.')
    parser.addoption('--clear', action='store_true',
                     help='destroy context at each tests done.')


def pytest_configure(config):
    globals()['config'] = config


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
        raise RuntimeError('Specify protocols to test:\n'
                           '--inproc|--ipc|--tcp|--pgm|--epgm or --all')
    elif 'pgm' in testing_protocols:
        # check CAP_NET_RAW
        try:
            socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        except socket.error as e:
            if e.errno == 1:  # Operation not permitted
                raise OSError('Enable the CAP_NET_RAW capability to use PGM:\n'
                              '$ sudo setcap CAP_NET_RAW=ep `which python`')
    return testing_protocols


def incremental_patience(f):
    if not config.option.incremental_patience:
        return f
    @functools.wraps(f)
    def patience_increased(**kwargs):
        patience = config.option.patience
        for x in xrange(5):
            kwargs['patience'] = patience
            try:
                return f(**kwargs)
            except:
                exctype, exc, traceback = sys.exc_info()
                patience *= 2
                warnings.warn('Patience increased to {0}'.format(patience))
        raise exctype, exc, traceback
    return patience_increased


customer_timeout = zeronimo.Customer.timeout
fanout_timeout = zeronimo.Fanout.timeout


def resolve_fixtures(f, protocol):
    @functools.wraps(f)
    @incremental_patience
    def fixture_resolved(**kwargs):
        ctx = zmq.Context()
        topic = rand_str()
        app = Application()
        patience = kwargs.pop('patience', config.option.patience)
        zeronimo.Customer.timeout = customer_timeout * patience
        zeronimo.Fanout.timeout = fanout_timeout * patience
        pull_addrs = set()
        sub_addrs = set()
        sub_socks = set()
        greenlets = set()
        socket_params = set()
        for param, val in kwargs.iteritems():
            if isinstance(val, deferred_worker):
                pull_sock = ctx.socket(zmq.PULL)
                pull_addr = gen_address(protocol)
                pull_sock.bind(pull_addr)
                pull_addrs.add(pull_addr)
                sub_sock = ctx.socket(zmq.SUB)
                sub_sock.set(zmq.SUBSCRIBE, topic)
                sub_addr = gen_address(protocol, fanout=True)
                sub_sock.bind(sub_addr)
                sub_addrs.add(sub_addr)
                sub_socks.add(sub_sock)
                worker_info = '{0}[{1}]({2})' \
                              ''.format(f.__name__, protocol, param)
                val = zeronimo.Worker(app, [pull_sock, sub_sock], worker_info)
                greenlets.add(val)
            elif isinstance(val, deferred_collector):
                pull_sock = ctx.socket(zmq.PULL)
                pull_addr = gen_address(protocol)
                pull_sock.bind(pull_addr)
                val = zeronimo.Collector(pull_sock, pull_addr)
                greenlets.add(val)
            elif isinstance(val, deferred_address):
                val = gen_address(protocol)
            elif isinstance(val, deferred_fanout_address):
                val = gen_address(protocol, fanout=True)
            elif isinstance(val, deferred_topic):
                val = topic
            elif isinstance(val, deferred_context):
                val = ctx
            elif isinstance(val, (deferred_push_socket, deferred_pub_socket)):
                socket_params.add(param)
            kwargs[param] = val
        for param in socket_params:
            val = kwargs[param]
            if isinstance(val, deferred_push_socket):
                sock_type = zmq.PUSH
                addrs = pull_addrs
            elif isinstance(val, deferred_pub_socket):
                sock_type = zmq.PUB
                addrs = sub_addrs
            else:
                assert 0
            sock = ctx.socket(sock_type)
            for addr in addrs:
                sock.connect(addr)
            if sock_type == zmq.PUB:
                sync_pubsub(sock, sub_socks, topic)
            kwargs[param] = sock
        for greenlet in greenlets:
            greenlet.start()
        try:
            return f(**kwargs)
        finally:
            for greenlet in greenlets:
                if greenlet.running():
                    greenlet.stop()
                try:
                    sockets = greenlet.sockets
                except AttributeError:
                    sockets = [greenlet.socket]
                for sock in sockets:
                    sock.close()
    return fixture_resolved


# decorate functions which use deferred fixtures with resolve_fixtures
# automatically.
import _pytest.python  # noqa
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
    msg = str(random.random())
    poller = zmq.Poller()
    for sub_sock in sub_socks:
        poller.register(sub_sock, zmq.POLLIN)
    to_sync = list(sub_socks)
    # sync all SUB sockets
    with gevent.Timeout(1, RuntimeError('Are SUB sockets subscribing?')):
        while to_sync:
            pub_sock.send_multipart([topic, msg])
            events = dict(poller.poll(timeout=1))
            for sub_sock in sub_socks:
                if sub_sock in events:
                    topic_recv, msg_recv = sub_sock.recv_multipart()
                    assert topic_recv == topic
                    assert msg_recv == msg
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
            topic_recv, msg_recv = sub_sock.recv_multipart()
            assert topic_recv == topic
            assert msg_recv == msg


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


@contextmanager
def running(greenlets, sockets=None):
    try:
        for greenlet in greenlets:
            greenlet.start()
        yield
    finally:
        for greenlet in greenlets:
            greenlet.stop()
        if sockets is not None:
            for sock in sockets:
                sock.close()


class Application(object):
    """The sample application."""

    def __new__(cls):
        obj = super(Application, cls).__new__(cls)
        counter = defaultdict(int)
        def count(f):
            @functools.wraps(f)
            def wrapped(*args, **kwargs):
                counter[f.__name__] += 1
                return f(*args, **kwargs)
            return wrapped
        for attr in dir(obj):
            if attr.endswith('__'):
                continue
            setattr(obj, attr, count(getattr(obj, attr)))
        obj.counter = counter
        return obj

    def zeronimo(self):
        return 'zeronimo'

    def add(self, a, b):
        """a + b."""
        return a + b

    def xrange(self, *args):
        return xrange(*args)

    def dict_view(self, *args):
        return dict((x, x) for x in xrange(*args)).viewkeys()

    def iter_xrange(self, *args):
        return iter(self.xrange(*args))

    def iter_dict_view(self, *args):
        return iter(self.dict_view(*args))

    def dont_yield(self):
        if False:
            yield 'it should\'t be sent'
            assert 0

    def rycbar123(self):
        for word in 'run, you clever boy; and remember.'.split():
            yield word

    def zero_div(self):
        0 / 0

    def rycbar123_and_zero_div(self):
        for word in self.rycbar123():
            yield word
        self.zero_div()

    def sleep(self, seconds):
        gevent.sleep(seconds)
        return seconds

    def sleep_multiple(self, seconds, times=1):
        for x in xrange(times):
            gevent.sleep(seconds)
            yield x

    def sleep_range(self, sleep, start, stop=None, step=1):
        if stop is None:
            start, stop = 0, start
        sequence = range(start, stop, step)
        for x, val in enumerate(sequence):
            yield val
            if x == len(sequence) - 1:
                # at the last
                break
            gevent.sleep(sleep)
