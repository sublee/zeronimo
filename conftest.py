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
import pytest
from singledispatch import singledispatch
import zmq.green as zmq

import zeronimo


TICK = 0.001
FEED_DIR = os.path.join(os.path.dirname(__file__), '_feeds')
WINDOWS = platform.system() == 'Windows'


config = NotImplemented


# fixtures


def_fixture = lambda name: namedtuple(name, ['protocol'])
worker_fixture = def_fixture('worker_fixture')
worker_pub_fixture = def_fixture('worker_pub_fixture')
collector_fixture = def_fixture('collector_fixture')
collector_sub_fixture = def_fixture('collector_sub_fixture')
customer_push_fixture = def_fixture('customer_push_fixture')
customer_pub_fixture = def_fixture('customer_pub_fixture')
address_fixture = def_fixture('address_fixture')
fanout_address_fixture = def_fixture('fanout_address_fixture')
topic_fixture = def_fixture('topic_fixture')
socket_fixture = def_fixture('socket_fixture')
reply_sockets_fixture = def_fixture('reply_sockets_fixture')
fixtures = {
    'worker*': worker_fixture,
    'worker_pub*': worker_pub_fixture,
    'collector*': collector_fixture,
    'push*': customer_push_fixture,
    'pub*': customer_pub_fixture,
    'addr*': address_fixture,
    'fanout_addr*': fanout_address_fixture,
    'topic': topic_fixture,
    'socket': socket_fixture,
    'reply_sockets': reply_sockets_fixture,
}
fanout_fixtures = set([
    worker_fixture, worker_pub_fixture, collector_fixture,
    customer_pub_fixture, fanout_address_fixture, reply_sockets_fixture,
])


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


def pytest_report_header(config, startdir):
    versions = (zmq.zmq_version(), zmq.__version__)
    print 'zmq: zmq-%s, pyzmq-%s' % versions


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
        has_fanout_fixtures = False
        for param in metafunc.fixturenames:
            for pattern, fixture in sorted(fixtures.items(),
                                           key=lambda (k, v): len(k),
                                           reverse=True):
                if fnmatch(param, pattern):
                    if fixture in fanout_fixtures:
                        has_fanout_fixtures = True
                    curargvalues.append(fixture(protocol))
                    break
            else:
                continue
            if not ids:
                argnames.append(param)
        if protocol.endswith('pgm') and not has_fanout_fixtures:
            continue
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
            if e.errno in [1, 2]:
                # [1] Operation not permitted
                # or
                # [2] No such file or directory (at the first time in PyPy)
                raise OSError('Enable the CAP_NET_RAW capability to use PGM:\n'
                              '$ sudo setcap CAP_NET_RAW=ep '
                              '$(readlink -f $(which python))')
    return testing_protocols


def incremental_patience(config):
    if not config.option.incremental_patience:
        return lambda f: f
    def decorator(f):
        @functools.wraps(f)
        def patience_increased(**kwargs):
            patience = config.option.patience
            for x in xrange(5):
                kwargs['patience'] = patience
                try:
                    return f(**kwargs)
                except KeyboardInterrupt:
                    raise
                except:
                    exctype, exc, traceback = sys.exc_info()
                    patience *= 2
                    warnings.warn('Patience increased to {0}'.format(patience))
            raise exctype, exc, traceback
        return patience_increased
    return decorator


customer_timeout = zeronimo.Customer.timeout
fanout_timeout = zeronimo.Fanout.timeout


def resolve_fixtures(f, request, protocol):
    config = request.config
    @functools.wraps(f)
    @incremental_patience(config)
    def fixture_resolved(**kwargs):
        ctx = zmq.Context.instance()
        request.session.addfinalizer(ctx.term)
        topic = rand_str()
        app = Application()
        patience = kwargs.pop('patience', config.option.patience)
        zeronimo.Customer.timeout = customer_timeout * patience
        zeronimo.Fanout.timeout = fanout_timeout * patience
        worker_pull_addrs = set()
        worker_sub_addrs = set()
        worker_pub_addrs = set()
        worker_sub_socks = set()
        worker_pub_socks = set()
        collector_sub_socks = {}
        reply_socks = set()
        backgrounds = set()
        socket_params = set()
        def make_socket(*args, **kwargs):
            sock = ctx.socket(*args, **kwargs)
            request.addfinalizer(sock.close)
            return sock
        def register_bg(bg):
            backgrounds.add(bg)
            request.addfinalizer(lambda: bg.stop(silent=True))
        # Resolve fixtures.
        @singledispatch
        def resolve_fixture(val, param):
            return val
        @resolve_fixture.register(worker_fixture)
        def resolve_worker_fixture(val, param):
            # PULL worker socket.
            pull_sock = make_socket(zmq.PULL)
            pull_addr = gen_address(protocol)
            pull_sock.bind(pull_addr)
            worker_pull_addrs.add(pull_addr)
            # SUB worker socket.
            sub_sock = make_socket(zmq.SUB)
            sub_sock.set(zmq.SUBSCRIBE, topic)
            sub_addr = gen_address(protocol, fanout=True)
            sub_sock.bind(sub_addr)
            worker_sub_addrs.add(sub_addr)
            worker_sub_socks.add(sub_sock)
            # PUB reply socket.
            pub_sock = make_socket(zmq.PUB)
            pub_addr = gen_address(protocol, fanout=True)
            pub_sock.bind(pub_addr)
            worker_pub_addrs.add(pub_addr)
            worker_pub_socks.add(pub_sock)
            # Make a worker.
            worker_info = '%s[%s](%s)' % (f.__name__, protocol, param)
            worker = zeronimo.Worker(app, [pull_sock, sub_sock], pub_sock,
                                     info=worker_info)
            register_bg(worker)
            return worker
        @resolve_fixture.register(collector_fixture)
        def resolve_collector_fixture(val, param):
            reply_topic = rand_str()
            sub_sock = make_socket(zmq.SUB)
            sub_sock.set(zmq.SUBSCRIBE, reply_topic)
            collector_sub_socks[reply_topic] = sub_sock
            collector = zeronimo.Collector(sub_sock, reply_topic)
            register_bg(collector)
            return collector
        @resolve_fixture.register(address_fixture)
        def resolve_address_fixture(val, param):
            return gen_address(protocol)
        @resolve_fixture.register(fanout_address_fixture)
        def resolve_fanout_address_fixture(val, param):
            return gen_address(protocol, fanout=True)
        @resolve_fixture.register(topic_fixture)
        def resolve_topic_fixture(val, param):
            return topic
        @resolve_fixture.register(socket_fixture)
        def resolve_socket_fixture(val, param):
            return make_socket
        @resolve_fixture.register(reply_sockets_fixture)
        def resolve_reply_sockets_fixture(val, param):
            def reply_sockets(count=1):
                # The sockets this function makes don't connect with other
                # worker or collector fixtures.
                addr = gen_address(protocol, fanout=True)
                reply_sock = make_socket(zmq.PUB)
                reply_sock.bind(addr)
                reply_socks.add(reply_sock)
                collector_sock_and_topics = []
                for x in range(count):
                    reply_topic = rand_str()
                    collector_sock = make_socket(zmq.SUB)
                    collector_sock.set(zmq.SUBSCRIBE, reply_topic)
                    collector_sock.connect(addr)
                    sync_pubsub(reply_sock, [collector_sock], reply_topic)
                    collector_sock_and_topics.append((collector_sock,
                                                      reply_topic))
                    reply_socks.add(collector_sock)
                return (reply_sock,) + tuple(collector_sock_and_topics)
            return reply_sockets
        @resolve_fixture.register(worker_pub_fixture)
        @resolve_fixture.register(collector_sub_fixture)
        @resolve_fixture.register(customer_push_fixture)
        @resolve_fixture.register(customer_pub_fixture)
        def resolve_socket_fixtures(val, param):
            socket_params.add(param)
            return val
        for param, val in kwargs.iteritems():
            if issubclass(val.__class__, object):
                kwargs[param] = resolve_fixture(val, param)
        # Resolve worker PUB fixtures.
        for param in socket_params:
            val = kwargs[param]
            if not isinstance(val, worker_pub_fixture):
                continue
            sock = make_socket(zmq.PUB)
            addr = gen_address(protocol, fanout=True)
            sock.bind(addr)
            worker_pub_addrs.add(addr)
            worker_pub_socks.add(sock)
            kwargs[param] = sock
        # Collectors connect to workers.
        for reply_topic, sock in collector_sub_socks.items():
            for addr in worker_pub_addrs:
                sock.connect(addr)
            for pub_sock in worker_pub_socks:
                sync_pubsub(pub_sock, [sock], reply_topic)
        # Resolve other socket fixtures.
        for param in socket_params:
            val = kwargs[param]
            if isinstance(val, collector_sub_fixture):
                sock = make_socket(zmq.SUB)
                connect_to_addrs(sock, worker_pub_addrs)
            elif isinstance(val, customer_push_fixture):
                sock = make_socket(zmq.PUSH)
                connect_to_addrs(sock, worker_pull_addrs)
            elif isinstance(val, customer_pub_fixture):
                sock = make_socket(zmq.PUB)
                connect_to_addrs(sock, worker_sub_addrs)
                sync_pubsub(sock, worker_sub_socks, topic)
            else:
                continue
            kwargs[param] = sock
        # Run the function.
        for bg in backgrounds:
            bg.start()
        try:
            return f(**kwargs)
        except:
            exc_info = sys.exc_info()
            raise exc_info[0], exc_info[1], exc_info[2].tb_next
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
                request = function._request
                function.obj = resolve_fixtures(function.obj,
                                                request, protocol)
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
    msg = rand_str()
    poller = zmq.Poller()
    for sub_sock in sub_socks:
        poller.register(sub_sock, zmq.POLLIN)
    to_sync = list(sub_socks)
    # sync all SUB sockets
    with gevent.Timeout(1, RuntimeError('are SUB sockets subscribing?')):
        while to_sync:
            pub_sock.send_multipart([topic, msg])
            events = dict(poller.poll(timeout=1))
            for sub_sock in sub_socks:
                if sub_sock not in events:
                    continue
                topic_recv, msg_recv = sub_sock.recv_multipart()
                assert topic_recv == topic
                if msg_recv != msg:
                    # When using EPGM, it can receive
                    # a message used at the previous.
                    continue
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


def connect_to_addrs(sock, addrs):
    for addr in addrs:
        sock.connect(addr)


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


@pytest.fixture
def device(fin):
    def f(*args, **kwargs):
        g = gevent.spawn(run_device, *args, **kwargs)
        fin(g.kill)
        g.join(0)
    return f


@pytest.fixture
def fin(request):
    return request.addfinalizer


@contextmanager
def running(backgrounds, sockets=()):
    try:
        for bg in backgrounds:
            bg.start(silent=True)
        yield
    finally:
        for bg in backgrounds:
            bg.stop()
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
                name = zeronimo.get_rpc_spec(f).name or f.__name__
                counter[name] += 1
                return f(*args, **kwargs)
            return wrapped
        for attr in dir(obj):
            if attr.endswith('__'):
                continue
            value = getattr(obj, attr)
            if not callable(value):
                continue
            setattr(obj, attr, count(value))
        obj.counter = counter
        return obj

    @zeronimo.rpc('zeronimo')
    def _zeronimo(self):
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

    def kwargs(self, **kwargs):
        return kwargs

    def is_remote(self):
        return False
    @zeronimo.rpc('is_remote')
    def _rpc_is_remote(self):
        return True

    @zeronimo.rpc(pass_call=True)
    def hints(self, call):
        return call.hints

    @zeronimo.rpc
    def reject_by_hints(self):
        return 'accepted'

    @reject_by_hints.reject_if
    def _reject_by_hints(self, call, topics):
        assert isinstance(self, Application)
        return 'reject' in call.hints

    @zeronimo.rpc
    def reject_by_hints_staticmethod(self):
        return 'accepted'

    @reject_by_hints_staticmethod.reject_if
    @staticmethod
    def _reject_by_hints_staticmethod(call, topics):
        return 'reject' in call.hints

    @zeronimo.rpc
    def reject_by_hints_classmethod(self):
        return 'accepted'

    @reject_by_hints_classmethod.reject_if
    @classmethod
    def _reject_by_hints_classmethod(cls, call, topics):
        assert isinstance(cls, type)
        return 'reject' in call.hints

    odd = False

    @zeronimo.rpc
    def reject_by_odd_even(self):
        return 'accepted'

    @reject_by_odd_even.reject_if
    def _reject_by_odd_even(self, call, topics):
        reject = self.odd
        self.odd = not self.odd
        return reject

    def raise_remote_value_error(self, value='zeronimo'):
        raise zeronimo.RemoteException.compose(ValueError)(value)
