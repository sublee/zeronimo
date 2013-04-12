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
from types import MethodType
import uuid

from gevent import joinall, spawn, Timeout
from gevent.coros import Semaphore
from gevent.event import Event
from gevent.queue import Queue, Empty
import zmq.green as zmq


__version__ = '0.0.dev'
__all__ = []


# utility functions


def alloc_id():
    return str(uuid.uuid4())


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


def zmq_send(sock, obj, flags=0, prefix='', dump=pickle.dumps):
    """Same with :meth:`zmq.Socket.send_pyobj` but can append prefix for
    filtering subscription.
    """
    msg = dump(obj)
    return sock.send(prefix + msg, flags)


def zmq_recv(sock, flags=0, prefix='', load=pickle.loads):
    """Same with :meth:`zmq.Socket.recv_pyobj`."""
    msg = sock.recv(flags)
    assert msg.startswith(prefix)
    return load(msg[len(prefix):])


# exceptions


class ZeronimoError(Exception):

    pass


# message frames


_Invocation = namedtuple(
    'Invocation',
    ['function_name', 'args', 'kwargs', 'invoker_id', 'customer_addr'])
_Reply = namedtuple(
    'Reply',
    ['method', 'data', 'invoker_id', 'task_id', 'worker_addr'])


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

    running_lock = None

    def __new__(cls, *args, **kwargs):
        obj = super(Runner, cls).__new__(cls)
        stopper = Event()
        cls._patch_run(obj, stopper)
        cls._patch_stop(obj, stopper)
        return obj

    @classmethod
    def _patch_run(cls, obj, stopper):
        obj.running_lock = Semaphore()
        def run(self):
            if self.is_running():
                return
            try:
                with self.running_lock:
                    rv = obj._run(lambda: not stopper.is_set())
            finally:
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
        return self.running_lock.locked()

    def ensure_running(self):
        if not self.is_running():
            spawn(self.run)

    def run(self, should_run):
        raise NotImplementedError

    def stop(self):
        self.running_lock.wait()


class ZMQSocketManager(object):
    """Manages ZeroMQ sockets."""

    context = None
    sock = None
    addr = None

    def __init__(self, context=None):
        if context is None:
            context = zmq.Context()
        self.context = context

    def open_sockets(self):
        """Opens all sockets if not opened."""
        if self.sock is not None:
            return
        self.sock = self.context.socket(zmq.PULL)
        if self.addr is not None:
            self.sock.bind(self.addr)

    def close_sockets(self):
        """Closes all sockets if not closed."""
        if self.sock is None:
            return
        self.sock.close()
        self.sock = None

    def bind(self, addr):
        """Binds an address to the socket."""
        if self.addr is not None:
            raise ValueError('Address already bound')
        if self.sock is not None:
            self.sock.bind(addr)
        self.addr = addr

    def unbind(self, addr):
        """Unbinds an address to the socket."""
        if self.sock is not None:
            self.sock.unbind(addr)
        self.addr = None


class Worker(Runner, ZMQSocketManager):

    obj = None
    fanout_sock = None
    fanout_addr = None
    prefix = None
    accepting = True

    def __init__(self, obj, bind=None, bind_fanout=None, prefix='', **kwargs):
        super(Worker, self).__init__(**kwargs)
        self.obj = obj
        bind and self.bind(bind)
        bind_fanout and self.bind_fanout(bind_fanout)
        self.subscribe(prefix)

    def open_sockets(self):
        super(Worker, self).open_sockets()
        if self.fanout_sock is not None:
            return
        self.fanout_sock = self.context.socket(zmq.SUB)
        if self.fanout_addr is not None:
            self.fanout_sock.bind(self.fanout_addr)
        if self.prefix is not None:
            self.fanout_sock.setsockopt(zmq.SUBSCRIBE, self.prefix)

    def close_sockets(self):
        super(Worker, self).close_sockets()
        if self.fanout_sock is None:
            return
        self.fanout_sock.close()
        self.fanout_sock = None

    def bind_fanout(self, addr):
        """Binds the address to SUB socket."""
        if self.fanout_addr is not None:
            raise ValueError('Address already bound')
        if self.fanout_sock is not None:
            self.fanout_sock.bind(addr)
        self.fanout_addr = addr

    def unbind_fanout(self, addr):
        """Unbinds the address to SUB socket."""
        if self.fanout_sock is not None:
            self.fanout_sock.unbind(addr)
        self.fanout_addr = None

    def subscribe(self, prefix):
        if self.fanout_sock:
            if self.prefix is not None:
                self.fanout_sock.setsockopt(zmq.UNSUBSCRIBE, self.prefix)
            self.fanout_sock.setsockopt(zmq.SUBSCRIBE, prefix)
        self.prefix = prefix

    def accept_all(self):
        self.accepting = True

    def reject_all(self):
        self.accepting = False

    def send_reply(self, sock, method, data, task_id, run_id, worker_addr):
        reply = Reply(method, data, task_id, run_id, worker_addr)
        #print 'worker send', reply
        return zmq_send(sock, reply)

    def run_task(self, invocation):
        task_id = alloc_id()
        function_name = invocation.function_name
        args = invocation.args
        kwargs = invocation.kwargs
        if invocation.customer_addr is None:
            sock = False
        else:
            sock = self.context.socket(zmq.PUSH)
            sock.connect(invocation.customer_addr)
            meta = (invocation.invoker_id, task_id, self.addr)
            method = ACCEPT if self.accepting else REJECT
            sock and self.send_reply(sock, method, None, *meta)
        if not self.accepting:
            return
        try:
            val = getattr(self.obj, function_name)(*args, **kwargs)
        except Exception as error:
            sock and self.send_reply(sock, RAISE, error, *meta)
            raise
        if should_yield(val):
            try:
                for item in val:
                    sock and self.send_reply(sock, YIELD, item, *meta)
            except Exception as error:
                sock and self.send_reply(sock, RAISE, error, *meta)
            else:
                sock and self.send_reply(sock, BREAK, None, *meta)
        else:
            sock and self.send_reply(sock, RETURN, val, *meta)

    def run(self, should_run):
        def serve(sock, prefix=''):
            while should_run():
                try:
                    spawn(self.run_task, zmq_recv(sock, prefix=prefix))
                except zmq.ZMQError:
                    continue
        self.open_sockets()
        try:
            joinall([spawn(serve, self.sock, self.prefix),
                     spawn(serve, self.fanout_sock, self.prefix)])
        finally:
            self.close_sockets()

    def stop(self):
        self.close_sockets()
        super(Worker, self).stop()

    def __repr__(self):
        return make_repr(self, ['addr', 'fanout_addr', 'prefix'])


class Customer(Runner, ZMQSocketManager):

    tunnels = None
    tasks = None

    def __init__(self, bind=None, reuse=False, **kwargs):
        super(Customer, self).__init__(**kwargs)
        self.tunnels = set()
        self.tasks = {}
        self.invokers = {}
        self._missings = {}
        bind and self.bind(bind)
        self.reuse = reuse

    def link(self, *args, **kwargs):
        return Tunnel(self, *args, **kwargs)

    def link_workers(self, workers, *args, **kwargs):
        """Merges addresses from the workers then creates a :class:`Tunnel`
        object. All workers should subscribe same topic.
        """
        addrs = []
        fanout_addrs = []
        prefix = None
        for worker in workers:
            addrs.append(worker.addr)
            fanout_addrs.append(worker.fanout_addr)
            if prefix is None:
                prefix = worker.prefix
            elif prefix != worker.prefix:
                raise ValueError('All workers should have same prefix')
        return Tunnel(self, addrs, fanout_addrs, prefix, *args, **kwargs)

    def register_tunnel(self, tunnel):
        """Registers the :class:`Tunnel` object to run and ensures a socket
        which pulls replies.
        """
        if tunnel in self.tunnels:
            raise ValueError('Already registered tunnel')
        self.tunnels.add(tunnel)

    def unregister_tunnel(self, tunnel):
        """Unregisters the :class:`Tunnel` object."""
        self.tunnels.remove(tunnel)
        if not self.reuse and self.is_running() and not self.tunnels:
            self.stop()

    def register_invoker(self, invoker):
        self.invokers[invoker.id] = invoker
        self.ensure_running()

    def unregister_invoker(self, invoker):
        assert self.invokers.pop(invoker.id) is invoker
        invoker.queue.put(StopIteration)

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

    def run(self, should_run):
        self.open_sockets()
        try:
            while should_run():
                try:
                    reply = zmq_recv(self.sock)
                except zmq.ZMQError:
                    continue
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
                        task = tasks[task_id]
                    except KeyError:
                        queue = self._missings[invoker_id][task_id]
                    else:
                        queue = task.queue
                #print 'customer recv', reply
                queue.put(reply)
        finally:
            self.close_sockets()

    def stop(self):
        self.close_sockets()
        super(Customer, self).stop()

    def __repr__(self):
        return make_repr(self, ['addr'])


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

    def __init__(self, customer, worker_addrs=None, worker_fanout_addrs=None,
                 worker_prefix='', **invoker_opts):
        self._znm_customer = customer
        self._znm_worker_addrs = set(worker_addrs or [])
        self._znm_worker_fanout_addrs = set(worker_fanout_addrs or [])
        self._znm_worker_prefix = worker_prefix
        self._znm_invoker_opts = invoker_opts
        self._znm_sockets = {}

    def __getattr__(self, attr):
        return functools.partial(self._znm_invoke, attr)

    def _znm_invoke(self, function_name, *args, **kwargs):
        """Invokes remote function."""
        invoker = Invoker(function_name, args, kwargs, self)
        return invoker.invoke(**self._znm_invoker_opts)

    def __enter__(self):
        self._znm_customer.register_tunnel(self)
        for socket_type, addrs in [(zmq.PUSH, self._znm_worker_addrs),
                                   (zmq.PUB, self._znm_worker_fanout_addrs)]:
            sock = self._znm_customer.context.socket(socket_type)
            for addr in addrs:
                sock.connect(addr)
            self._znm_sockets[socket_type] = sock
        return self

    def __exit__(self, error, error_type, traceback):
        for sock in self._znm_sockets.viewvalues():
            sock.close()
        self._znm_sockets.clear()
        self._znm_customer.unregister_tunnel(self)

    def __call__(self, **replacing_invoker_opts):
        """Creates a :class:`Tunnel` object which follows same consumer and
        workers but replaced invoker options.
        """
        invoker_opts = {}
        invoker_opts.update(self._znm_invoker_opts)
        invoker_opts.update(replacing_invoker_opts)
        tunnel = Tunnel(self._znm_customer, self._znm_worker_addrs,
                        self._znm_worker_fanout_addrs, self._znm_worker_prefix,
                        **invoker_opts)
        tunnel._znm_sockets = self._znm_sockets
        return tunnel

    def __repr__(self):
        params =['customer', 'worker_addrs', 'worker_fanout_addrs',
                 'worker_prefix']
        keywords = self._znm_invoker_opts.keys()
        attrs = params + keywords
        data = {attr: getattr(self, '_znm_' + attr) for attr in params}
        data.update(self._znm_invoker_opts)
        return make_repr(self, params, keywords, data)


class Invoker(object):

    def __init__(self, function_name, args, kwargs, tunnel, id=None):
        self.function_name = function_name
        self.args = args
        self.kwargs = kwargs
        self.tunnel = tunnel
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
        #print 'invoker.invoke_nowait send', invocation
        zmq_send(sock, invocation, prefix=self.worker_prefix)

    def _invoke(self, as_task=False, finding_timeout=0.01):
        sock = self.sockets[zmq.PUSH]
        invocation = Invocation(self.function_name, self.args, self.kwargs,
                                self.id, self.customer.addr)
        # find one worker
        self.customer.register_invoker(self)
        rejected = set()
        try:
            with Timeout(finding_timeout, False):
                while True:
                    #print 'invoker.invoke send', invocation
                    zmq_send(sock, invocation, prefix=self.worker_prefix)
                    reply = None
                    reply = self.queue.get()
                    if reply.method == REJECT:
                        rejected.add(reply.worker_addr)
                        continue
                    elif reply.method == ACCEPT:
                        break
                    assert 0
        finally:
            self.customer.unregister_invoker(self)
        if reply is None:
            errmsg = 'Worker not found'
            if rejected:
                errmsg += ', {0} worker{1} rejected'.format(
                    len(rejected), '' if len(rejected) == 1 else 's')
            raise ZeronimoError(errmsg)
        return self._spawn_task(reply, as_task)

    def _spawn_task(self, reply, as_task=False):
        assert reply.method == ACCEPT
        task = Task(self.customer, reply.task_id, self.id, reply.worker_addr)
        return task if as_task else task()

    def _invoke_fanout(self, as_task=False, finding_timeout=0.01):
        sock = self.sockets[zmq.PUB]
        invocation = Invocation(self.function_name, self.args, self.kwargs,
                                self.id, self.customer.addr)
        # find one or more workers
        self.customer.register_invoker(self)
        replies = []
        rejected = 0
        #print 'invoker.invoke_fanout send', invocation
        zmq_send(sock, invocation, prefix=self.worker_prefix)
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
            if rejected:
                errmsg += ', {0} worker{1} rejected'.format(
                    rejected, '' if rejected == 1 else 's')
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
                task = Task(self.customer, reply.task_id, self.id,
                            reply.worker_addr)
                tasks.append(task if as_task else task())
        collect_tasks(iter_replies.next)
        spawn(collect_tasks, self.queue.get)
        return tasks

    def __repr__(self):
        return make_repr(self, ['function_name', 'args', 'kwargs'])


class Task(object):

    def __init__(self, customer, id=None, invoker_id=None, worker_addr=None):
        self.customer = customer
        self.id = id
        self.invoker_id = invoker_id
        self.worker_addr = worker_addr
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
            self, ['customer'], ['id', 'invoker_id', 'worker_addr'])
