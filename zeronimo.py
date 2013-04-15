# -*- coding: utf-8 -*-
"""
    zeronimo
    ~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import (
    defaultdict, namedtuple, Iterable, Sequence, Set, Mapping)
import functools
try:
    import cPickle as pickle
except ImportError:
    import pickle
import re
from types import MethodType
import uuid

from gevent import spawn, joinall, killall, Timeout
from gevent.coros import Semaphore
from gevent.event import Event, AsyncResult
from gevent.queue import Queue, Empty
import zmq.green as zmq


__version__ = '0.0.dev'
__all__ = []
from termcolor import colored


# utility functions


def alloc_id():
    return str(uuid.uuid4())[:6]


def should_yield(val):
    return (
        isinstance(val, Iterable) and
        not isinstance(val, (Sequence, Set, Mapping)))


def assort_addrs(addrs):
    assorted_addrs = defaultdict(set)
    for addr in addrs:
        protocol = addr.split('://', 1)[0]
        assorted_addrs[protocol].add(addr)
    return dict(assorted_addrs)


def best_addr(host_addrs, peer_addrs):
    return iter(host_addrs).next()
    priorities = {'inproc': 0, 'ipc': 1, 'tcp': 2, 'pgm': 2, 'epgm': 2}
    assorted_host_addrs = assort_addrs(host_addrs)
    assorted_peer_addrs = assort_addrs(peer_addrs)
    for host_protocol, addrs in assorted_host_addrs.viewitems():
        host_priority = priorities[host_protocol]
        for peer_protocol in assorted_peer_addrs.viewkeys():
            peer_priority = priorities[peer_protocol]
            if peer_priority <= host_priority:
                return iter(addrs).next()
    raise ValueError('Cannot find the best address the peer is connectable')


def ensure_sequence(val, sequence=list):
    if val is None:
        return sequence()
    elif isinstance(val, (Sequence, Set)):
        return sequence(val)
    else:
        return sequence([val])


def make_repr(obj, params=[], keywords=[], data={}):
    get = lambda attr: data[attr] if attr in data else getattr(obj, attr)
    c = type(obj)
    opts = []
    if params:
        opts.append(', '.join([repr(get(attr)) for attr in params]))
    if keywords:
        opts.append(', '.join(
            ['{0}={1!r}'.format(attr, get(attr)) for attr in keywords]))
    return '{0}({1})'.format(c.__name__, ', '.join(opts))


# wrapped ZMQ functions


prefix_length_pattern = re.compile(r'\n(\d+)$')


def zmq_send(sock, obj, flags=0, prefix='', dump=pickle.dumps):
    """Same with :meth:`zmq.Socket.send_pyobj` but can append prefix for
    filtering subscription.
    """
    msg = '{0}\n{1}\n{2}'.format(prefix, dump(obj), len(prefix))
    return sock.send(msg, flags)


def zmq_recv(sock, flags=0, load=pickle.loads):
    """Same with :meth:`zmq.Socket.recv_pyobj`."""
    msg = sock.recv(flags)
    prefix_length_match = prefix_length_pattern.search(msg)
    prefix_length = int(prefix_length_match.group(1))
    obj = load(msg[prefix_length + 1:-len(prefix_length_match.group(0))])
    return obj


# exceptions


class ZeronimoError(Exception):

    pass


# message frames


_Invocation = namedtuple('Invocation', ['function_name', 'args', 'kwargs',
                                        'invoker_id', 'customer_addr'])
_Reply = namedtuple('Reply', ['method', 'data', 'invoker_id', 'task_id'])


class Invocation(_Invocation):

    def __repr__(self):
        return make_repr(self, keywords=self._fields)


class Reply(_Reply):

    def __repr__(self):
        method = {1: 'ACCEPT', 0: 'REJECT',
                  100: 'RETURN', 101: 'RAISE',
                  102: 'YIELD', 103: 'BREAK'}[self.method]
        class M(object):
            def __repr__(self):
                return method
        return make_repr(self, keywords=self._fields, data={'method': M()})


# reply methods


ACCEPT = 1
REJECT = 0
RETURN = 100
RAISE = 101
YIELD = 102
BREAK = 103


# models


class Runner(object):
    """A runner should implement :meth:`run`. :attr:`running` is ensured to be
    ``True`` while :meth:`run` is runnig.
    """

    def __new__(cls, *args, **kwargs):
        obj = super(Runner, cls).__new__(cls)
        stopper = Event()
        cls._patch_run(obj, stopper)
        cls._patch_stop(obj, stopper)
        return obj

    @classmethod
    def _patch_run(cls, obj, stopper):
        obj._running_lock = Semaphore()
        def run(self):
            if self.is_running():
                raise RuntimeError('{0} is already running'.format(self))
            try:
                with self._running_lock:
                    rv = obj._run(stopper)
            finally:
                try:
                    del self._async_running
                except AttributeError:
                    pass
                stopper.clear()
            return rv
        obj.run, obj._run = MethodType(run, obj), obj.run

    @classmethod
    def _patch_stop(cls, obj, stopper):
        def stop(self):
            if not self.is_running():
                raise RuntimeError('{0} not running'.format(cls.__name__))
            stopper.set()
            return obj._stop()
        obj.stop, obj._stop = MethodType(stop, obj), obj.stop

    def is_running(self):
        return self._running_lock.locked()

    def run(self, stopper):
        raise NotImplementedError

    def start(self):
        if self.is_running():
            raise RuntimeError('{0} is already running'.format(self))
        self._async_running = spawn(self.run)

    def stop(self):
        self.wait()

    def join(self, block=True, timeout=None):
        try:
            return self._async_running.get(block, timeout)
        except AttributeError:
            raise RuntimeError('Running in foreground')

    def wait(self, timeout=None):
        try:
            return self._async_running.wait(timeout)
        except AttributeError:
            self._running_lock.wait(timeout)


class Worker(Runner):

    def __init__(self, obj, sockets, info=None):
        super(Worker, self).__init__()
        self.obj = obj
        self.sockets = sockets
        self.info = info
        self.accept_all()

    def accept_all(self):
        self.accepting = True

    def reject_all(self):
        self.accepting = False

    def run(self, stopper):
        poller = zmq.Poller()
        for sock in self.sockets:
            poller.register(sock, zmq.POLLIN)
        def poll_or_stopped():
            async_result = AsyncResult()
            spawn(stopper.wait).link(async_result)
            spawn(poller.poll).link(async_result)
            return async_result.get()
        while not stopper.is_set():
            events = poll_or_stopped()
            if events is True:  # has been stopped
                break
            for sock, event in events:
                spawn(self.run_task, zmq_recv(sock), sock.context)

    def run_task(self, invocation, context):
        task_id = alloc_id()
        #print colored('worker recv ' + repr(invocation), 'cyan')
        function_name = invocation.function_name
        args = invocation.args
        kwargs = invocation.kwargs
        try:
            if invocation.customer_addr is None:
                sock = False
            else:
                sock = context.socket(zmq.PUSH)
                sock.connect(invocation.customer_addr)
                channel = (invocation.invoker_id, task_id)
                method = ACCEPT if self.accepting else REJECT
                sock and self.send_reply(sock, method, self.info, *channel)
            if not self.accepting:
                return
            try:
                val = getattr(self.obj, function_name)(*args, **kwargs)
            except Exception as error:
                sock and self.send_reply(sock, RAISE, error, *channel)
                raise
            if should_yield(val):
                try:
                    for item in val:
                        sock and self.send_reply(sock, YIELD, item, *channel)
                except Exception as error:
                    sock and self.send_reply(sock, RAISE, error, *channel)
                else:
                    sock and self.send_reply(sock, BREAK, None, *channel)
            else:
                sock and self.send_reply(sock, RETURN, val, *channel)
        finally:
            sock and sock.close()

    def send_reply(self, sock, method, data, task_id, run_id):
        reply = Reply(method, data, task_id, run_id)
        #print 'worker send', reply
        return zmq_send(sock, reply)

    def __repr__(self):
        keywords = ['info'] if self.info is not None else []
        return make_repr(self, ['obj', 'sockets'], keywords)


class Customer(Runner):

    tunnels = None
    tasks = None

    def __init__(self, public_addr, socket):
        super(Customer, self).__init__()
        self.public_addr = public_addr
        self.socket = socket
        self.tunnels = set()
        self.tasks = {}
        self.invokers = {}
        self._missings = {}

    def link(self, *args, **kwargs):
        return Tunnel(self, *args, **kwargs)

    def register_tunnel(self, tunnel):
        """Registers the :class:`Tunnel` object to run and ensures a socket
        which pulls replies.
        """
        if tunnel in self.tunnels:
            raise ValueError('Already registered tunnel')
        self.tunnels.add(tunnel)
        return self.tunnels

    def unregister_tunnel(self, tunnel):
        """Unregisters the :class:`Tunnel` object."""
        self.tunnels.remove(tunnel)
        return self.tunnels

    def register_invoker(self, invoker):
        self.invokers[invoker.id] = invoker
        return self.invokers

    def unregister_invoker(self, invoker):
        assert self.invokers.pop(invoker.id) is invoker
        invoker.queue.put(StopIteration)
        return self.invokers

    def register_task(self, task):
        try:
            self.tasks[task.invoker_id][task.id] = task
        except KeyError:
            self.tasks[task.invoker_id] = {task.id: task}
        self._restore_missing_messages(task)

    def unregister_task(self, task):
        assert self.tasks[task.invoker_id].pop(task.id) is task
        if self.tasks[task.invoker_id]:
            return
        try:
            self.unregister_invoker(self.invokers[task.invoker_id])
        except KeyError:
            pass
        del self.tasks[task.invoker_id]

    def _restore_missing_messages(self, task):
        try:
            missing = self._missings[task.invoker_id].pop(task.id)
        except KeyError:
            return
        if not self._missings[task.invoker_id]:
            del self._missings[task.invoker_id]
        try:
            while missing.queue:
                task.queue.put(missing.get(block=False))
        except Empty:
            pass

    def run(self, stopper):
        def safe_zmq_recv(*args, **kwargs):
            try:
                return zmq_recv(*args, **kwargs)
            except zmq.ZMQError:
                pass
        def recv_or_stopped():
            async_result = AsyncResult()
            stopper_checking = spawn(stopper.wait)
            receiving = spawn(safe_zmq_recv, self.socket)
            stopper_checking.link(async_result)
            receiving.link(async_result)
            try:
                return async_result.get()
            finally:
                killall([stopper_checking, receiving])
        while not stopper.is_set():
            try:
                reply = recv_or_stopped()
            except zmq.ZMQError:
                continue
            if reply is True:  # has been stopped
                break
            invoker_id = reply.invoker_id
            task_id = reply.task_id
            if reply.method in (ACCEPT, REJECT):
                try:
                    queue = self.invokers[invoker_id].queue
                except KeyError:
                    # drop message
                    continue
                # prepare collections for task messages
                if invoker_id not in self.tasks:
                    self.tasks[invoker_id] = {}
                try:
                    self._missings[invoker_id][task_id] = Queue()
                except KeyError:
                    self._missings[invoker_id] = {task_id: Queue()}
            else:
                try:
                    tasks = self.tasks[invoker_id]
                except KeyError:
                    # drop message
                    continue
                try:
                    queue = tasks[task_id].queue
                except KeyError:
                    queue = self._missings[invoker_id][task_id]
            #print 'customer recv', reply
            queue.put(reply)

    def __repr__(self):
        return make_repr(self, ['public_addr'])


class Tunnel(object):
    """A session between the customer and the distributed workers. It can send
    a request of RPC through sockets on the customer's context.

    :param customer: the :class:`Customer` object.
    :param addrs: the destination worker addresses bound at PULL sockets.
    :param fanout_addrs: the destination worker addresses bound at SUB sockets.
    :param prefix: the filter the workers are subscribing.

    :param wait: if it's set to ``True``, the workers will reply. Otherwise,
                 the workers just invoke a function without reply. Defaults to
                 ``True``.
    :param fanout: if it's set to ``True``, all workers will receive an
                   invocation request. Defaults to ``False``.
    :param as_task: actually, every remote function calls have own
                    :class:`Task` object. if it's set to ``True``, remote
                    functions return a :class:`Task` object instead of result
                    value. Defaults to ``False``.
    :param timeout: the seconds to timeout for collecting workers which
                    accepted a task. Defaults to 0.01 seconds.
    """

    def __init__(self, customer, push_sock=None, pub_sock=None, prefix='',
                 **invoker_opts):
        self._znm_customer = customer
        self._znm_sockets = {}
        if push_sock is not None:
            assert push_sock.socket_type == zmq.PUSH
            self._znm_sockets[zmq.PUSH] = push_sock
        if pub_sock is not None:
            assert pub_sock.socket_type == zmq.PUB
            self._znm_sockets[zmq.PUB] = pub_sock
        self._znm_prefix = prefix
        self._znm_invoker_opts = invoker_opts

    def __getattr__(self, attr):
        return functools.partial(self._znm_invoke, attr)

    def _znm_invoke(self, function_name, *args, **kwargs):
        """Invokes a remote function."""
        invoker = Invoker(self, function_name, args, kwargs)
        return invoker.invoke(**self._znm_invoker_opts)

    def __enter__(self):
        customer = self._znm_customer
        if customer.register_tunnel(self) and not customer.is_running():
            customer.start()
        return self

    def __exit__(self, error, error_type, traceback):
        customer = self._znm_customer
        if not customer.unregister_tunnel(self):
            customer.stop()

    def __call__(self, **replacing_invoker_opts):
        """Creates a :class:`Tunnel` object which follows same consumer and
        workers but replaced invoker options.
        """
        invoker_opts = {}
        invoker_opts.update(self._znm_invoker_opts)
        invoker_opts.update(replacing_invoker_opts)
        tunnel = Tunnel(self._znm_customer, self._znm_sockets.get(zmq.PUSH),
                        self._znm_sockets.get(zmq.PUB), self._znm_prefix,
                        **invoker_opts)
        tunnel._znm_sockets = self._znm_sockets
        return tunnel

    def __repr__(self):
        params =['customer']
        keywords = self._znm_invoker_opts.keys()
        attrs = params + keywords
        data = {attr: getattr(self, '_znm_' + attr) for attr in params}
        data.update(self._znm_invoker_opts)
        return make_repr(self, params, keywords, data)


class Invoker(object):

    def __init__(self, tunnel, function_name, args, kwargs, id=None):
        self.tunnel = tunnel
        self.function_name = function_name
        self.args = args
        self.kwargs = kwargs
        self.id = alloc_id() if id is None else id
        self.queue = Queue()

    def __getattr__(self, attr):
        return getattr(self.tunnel, '_znm_' + attr)

    def invoke(self, wait=True, fanout=False, as_task=False,
               finding_timeout=0.01):
        if not wait:
            return self._invoke_nowait(fanout)
        if fanout:
            return self._invoke_fanout(as_task, finding_timeout)
        else:
            return self._invoke(as_task, finding_timeout)

    def _invoke_nowait(self, fanout=False):
        sock = self.sockets[zmq.PUB if fanout else zmq.PUSH]
        invocation = Invocation(
            self.function_name, self.args, self.kwargs, self.id, None)
        #print colored('invoker.invoke_nowait send ' + repr(invocation), 'magenta')
        zmq_send(sock, invocation, prefix=self.prefix)

    def _invoke(self, as_task=False, finding_timeout=0.01):
        sock = self.sockets[zmq.PUSH]
        invocation = Invocation(self.function_name, self.args, self.kwargs,
                                self.id, self.customer.public_addr)
        # find one worker
        self.customer.register_invoker(self)
        rejected = 0
        try:
            with Timeout(finding_timeout, False):
                while True:
                    #print colored('invoker.invoke send ' + repr(invocation), 'magenta')
                    zmq_send(sock, invocation, prefix=self.prefix)
                    reply = None
                    reply = self.queue.get()
                    if reply.method == REJECT:
                        rejected += 1
                        continue
                    elif reply.method == ACCEPT:
                        break
                    assert 0
        finally:
            self.customer.unregister_invoker(self)
        if reply is None:
            errmsg = 'Worker not found'
            if rejected == 1:
                errmsg += ', a worker rejected'
            elif rejected:
                errmsg += ', {0} workers rejected'.format(rejected)
            raise ZeronimoError(errmsg)
        return self._spawn_task(reply, as_task)

    def _spawn_task(self, reply, as_task=False):
        assert reply.method == ACCEPT
        task = Task(self.customer, reply.task_id, self.id, reply.data)
        return task if as_task else task()

    def _invoke_fanout(self, as_task=False, finding_timeout=0.01):
        sock = self.sockets[zmq.PUB]
        invocation = Invocation(self.function_name, self.args, self.kwargs,
                                self.id, self.customer.public_addr)
        # find one or more workers
        self.customer.register_invoker(self)
        replies = []
        rejected = 0
        #print colored('invoker.invoke_fanout send ' + repr(invocation), 'magenta')
        zmq_send(sock, invocation, prefix=self.prefix)
        with Timeout(finding_timeout, False):
            while True:
                reply = self.queue.get()
                if reply.method == REJECT:
                    rejected += 1
                elif reply.method == ACCEPT:
                    replies.append(reply)
                else:
                    assert 0
        if not replies:
            errmsg = 'Worker not found'
            if rejected == 1:
                errmsg += ', a worker rejected'
            elif rejected:
                errmsg += ', {0} workers rejected'.format(rejected)
            raise ZeronimoError(errmsg)
        return self._spawn_fanout_tasks(replies, as_task)

    def _spawn_fanout_tasks(self, replies, as_task=False):
        tasks = []
        iter_replies = iter(replies)
        def collect_tasks(getter):
            for reply in iter(getter, None):
                if reply is StopIteration:
                    break
                assert reply.method == ACCEPT
                task = Task(self.customer, reply.task_id, self.id, reply.data)
                tasks.append(task if as_task else task())
        collect_tasks(iter_replies.next)
        spawn(collect_tasks, self.queue.get)
        return tasks

    def __repr__(self):
        return make_repr(self, ['function_name', 'args', 'kwargs'])


class Task(object):

    def __init__(self, customer, id=None, invoker_id=None, worker_info=None):
        self.customer = customer
        self.id = id
        self.invoker_id = invoker_id
        self.worker_info = worker_info
        self.queue = Queue()

    def __call__(self):
        self.customer.register_task(self)
        reply = self.queue.get()
        #print 'task recv %r' % (reply,)
        assert reply.method not in (ACCEPT, REJECT)
        if reply.method in (RETURN, RAISE):
            self.customer.unregister_task(self)
        if reply.method == RETURN:
            return reply.data
        elif reply.method == RAISE:
            raise reply.data
        elif reply.method == YIELD:
            return self.remote_iterator(reply)
        elif reply.method == BREAK:
            return iter([])

    def remote_iterator(self, first_reply):
        yield first_reply.data
        while True:
            reply = self.queue.get()
            #print 'task recv %r' % (reply,)
            assert reply.method not in (ACCEPT, REJECT, RETURN)
            if reply.method == YIELD:
                yield reply.data
            elif reply.method == RAISE:
                raise reply.data
            elif reply.method == BREAK:
                break

    def __repr__(self):
        return make_repr(
            self, ['customer'], ['id', 'invoker_id', 'worker_info'])
