# -*- coding: utf-8 -*-
"""
    zeronimo
    ~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import (
    defaultdict, namedtuple, Iterable, Sequence, Set, Mapping)
from contextlib import contextmanager, nested
import functools
import hashlib
try:
    import cPickle as pickle
except ImportError:
    import pickle
from types import MethodType
import uuid

from gevent import joinall, spawn, Timeout
from gevent.coros import Semaphore
from gevent.event import AsyncResult
from gevent.queue import Queue, Empty
import zmq.green as zmq


__version__ = '0.0.dev'
__all__ = []


# exceptions


class ZeronimoError(RuntimeError): pass
class ZeronimoWarning(RuntimeWarning): pass
class AcceptanceError(ZeronimoError): pass


# message frames


class Invocation(namedtuple('Invocation', [
    'name', 'args', 'kwargs', 'task_id', 'customer_addr'])):

    def __repr__(self):
        args = (type(self).__name__,) + self
        return '{}({!r}, {}, {}, {!r}, {!r})'.format(*args)


class Reply(namedtuple('Reply', [
    'method', 'data', 'task_id', 'run_id', 'worker_addr'])):

    def __repr__(self):
        method = {1: 'ACCEPT', 0: 'REJECT',
                  10: 'RETURN', 11: 'RAISE',
                  12: 'YIELD', 13: 'BREAK'}[self.method]
        args = (type(self).__name__, method) + self[1:]
        return '{}({}, {!r}, {!r}, {!r}, {!r})'.format(*args)


# reply methods
ACCEPT = 1
REJECT = 0
RETURN = 10
RAISE = 11
YIELD = 12
BREAK = 13


def uuid_str():
    return hashlib.md5(str(uuid.uuid4())).hexdigest()[:6]


def remote(func):
    """This decorator makes a function to be collected by
    :func:`collect_remote_functions` for being invokable by remote clients.
    """
    func._znm = True
    return func


def collect_remote_functions(obj):
    """Collects remote functions from the object."""
    functions = {}
    for attr in dir(obj):
        func = getattr(obj, attr)
        if hasattr(func, '_znm') and func._znm:
            functions[attr] = func
    return functions


def should_yield(val):
    return (
        isinstance(val, Iterable) and
        not isinstance(val, (Sequence, Set, Mapping)))


def ensure_sequence(val, sequence=list):
    if val is None:
        return sequence()
    elif isinstance(val, (Sequence, Set)):
        return sequence(val)
    else:
        return sequence([val])


# ZMQ messaging functions


def send(sock, obj, flags=0, prefix=''):
    """Same with :meth:`zmq.Socket.send_pyobj` but can append prefix for
    filtering subscription.
    """
    msg = pickle.dumps(obj)
    return sock.send(prefix + msg, flags)


def recv(sock, flags=0):
    """Same with :meth:`zmq.Socket.recv_pyobj`."""
    msg = sock.recv(flags)
    return pickle.loads(msg)


class Base(object):
    """Manages ZeroMQ sockets."""

    running = 0
    context = None
    sock = None
    addrs = None

    def __new__(cls, *args, **kwargs):
        obj = super(Base, cls).__new__(cls)
        obj._running_lock = Semaphore()
        def run(self):
            if self._running_lock.locked():
                return
            try:
                with self._running_lock:
                    obj.running += 1
                    rv = obj._run()
            finally:
                obj.running -= 1
                assert obj.running >= 0
            return rv
        obj.run, obj._run = MethodType(run, obj), obj.run
        return obj

    def __init__(self, context=None):
        self.context = context
        self.reset_sockets()

    def __del__(self):
        self.running = 0

    def reset_sockets(self):
        if self.sock is not None:
            self.sock.close()
        self.sock = self.context.socket(zmq.PULL)
        self.addrs = set()

    def bind(self, addr):
        self.sock.bind(addr)
        self.addrs.add(addr)

    def unbind(self, addr):
        self.sock.unbind(addr)
        self.addrs.remove(addr)

    def run(self):
        raise NotImplementedError


class Worker(Base):

    obj = None
    fanout_sock = None
    fanout_addrs = None
    fanout_topic = None
    accepting = True

    def __init__(self, obj, bind=None, bind_fanout=None, fanout_topic='',
                 **kwargs):
        super(Worker, self).__init__(**kwargs)
        self.obj = obj
        bind and self.bind(bind)
        bind_fanout and self.bind_fanout(bind_fanout)
        self.subscribe(fanout_topic)

    def reset_sockets(self):
        """Resets PULL and SUB sockets."""
        super(Worker, self).reset_sockets()
        if self.fanout_sock is not None:
            self.fanout_sock.close()
        self.fanout_sock = self.context.socket(zmq.SUB)
        self.fanout_addrs = set()

    def bind_fanout(self, addr):
        """Binds the address to SUB socket."""
        self.fanout_sock.bind(addr)
        self.fanout_addrs.add(addr)

    def unbind_fanout(self, addr):
        """Unbinds the address to SUB socket."""
        self.fanout_sock.unbind(addr)
        self.fanout_addrs.remove(addr)

    def subscribe(self, fanout_topic):
        if self.fanout_topic is not None:
            self.fanout_sock.setsockopt(zmq.UNSUBSCRIBE, self.fanout_topic)
        self.fanout_sock.setsockopt(zmq.SUBSCRIBE, fanout_topic)
        self.fanout_topic = fanout_topic

    def accept_all(self):
        self.accepting = True

    def reject_all(self):
        self.accepting = False

    def run_task(self, invocation):
        run_id = uuid_str()
        meta = (invocation.task_id, run_id, list(self.addrs)[0])
        name = invocation.name
        args = invocation.args
        kwargs = invocation.kwargs
        print 'worker recv %r (run_id=%s)' % (invocation, run_id)
        if invocation.customer_addr is None:
            sock = False
        else:
            sock = self.context.socket(zmq.PUSH)
            sock.connect(invocation.customer_addr)
            method = ACCEPT if self.accepting else REJECT
            send(sock, Reply(method, None, *meta))
        if not self.accepting:
            print 'task rejected'
            return
        try:
            val = getattr(self.obj, name)(*args, **kwargs)
        except Exception, error:
            print 'worker send %r' % (Reply(RAISE, error, *meta),)
            sock and send(sock, Reply(RAISE, error, *meta))
            raise
        if should_yield(val):
            try:
                for item in val:
                    print 'worker send %r' % (Reply(YIELD, item, *meta),)
                    sock and send(sock, Reply(YIELD, item, *meta))
            except Exception, error:
                print 'worker send %r' % (Reply(RAISE, error, *meta),)
                sock and send(sock, Reply(RAISE, error, *meta))
            else:
                print 'worker send %r' % (Reply(BREAK, None, *meta),)
                sock and send(sock, Reply(BREAK, None, *meta))
        else:
            print 'worker send %r' % (Reply(RETURN, val, *meta),)
            sock and send(sock, Reply(RETURN, val, *meta))

    def run(self):
        def serve(sock):
            while self.running:
                spawn(self.run_task, recv(sock))
        joinall([spawn(serve, self.sock), spawn(serve, self.fanout_sock)])


class Customer(Base):

    tunnels = None
    tasks = None

    def __init__(self, bind=None, **kwargs):
        super(Customer, self).__init__(**kwargs)
        self.tunnels = set()
        self.tasks = {}
        self._missing_tasks = {}
        bind and self.bind(bind)

    def link(self, *args, **kwargs):
        return Tunnel(self, *args, **kwargs)

    def link_workers(self, workers, *args, **kwargs):
        """Merges addresses from the workers then creates a :class:`Tunnel`
        object. All workers should subscribe same topic.
        """
        addrs = set()
        fanout_addrs = set()
        fanout_topic = None
        for worker in workers:
            addrs.update(worker.addrs)
            fanout_addrs.update(worker.fanout_addrs)
            if fanout_topic is None:
                fanout_topic = worker.fanout_topic
            elif fanout_topic != worker.fanout_topic:
                raise ValueError('All workers should subscribe same topic')
        return Tunnel(self, addrs, fanout_addrs, fanout_topic, *args, **kwargs)

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
        if self.sock is not None and not self.tunnels:
            self.sock.close()

    def register_task(self, task):
        try:
            self.tasks[task.id][task.run_id] = task
        except KeyError:
            self.tasks[task.id] = {task.run_id: task}
        self._restore_missing_messages(task)

    def unregister_task(self, task):
        assert self.tasks[task.id].pop(task.run_id) is task
        if task.run_id is None or not self.tasks[task.id]:
            del self.tasks[task.id]

    def _restore_missing_messages(self, task):
        try:
            missing = self._missing_tasks[task.id].pop(task.run_id)
        except KeyError:
            return
        if not self._missing_tasks[task.id]:
            del self._missing_tasks[task.id]
        try:
            while missing.queue:
                task.queue.put(missing.queue.get(block=False))
        except Empty:
            pass

    def run(self):
        while self.tunnels:
            try:
                reply = recv(self.sock)
            except zmq.ZMQError:
                continue
            task_id = reply.task_id
            run_id = reply.run_id
            if reply.method in (ACCEPT, REJECT):
                run_id = None
            try:
                tasks = self.tasks[task_id]
            except KeyError:
                # drop message
                continue
            try:
                task = tasks[run_id]
            except KeyError:
                try:
                    task = self._missing_tasks[task_id][run_id]
                except KeyError:
                    # tasks to collect missing messages
                    task = Task(None, task_id, run_id)
                    if task_id not in self._missing_tasks:
                        self._missing_tasks[task_id] = {run_id: task}
                    elif run_id not in self._missing_tasks[task_id]:
                        self._missing_tasks[task_id][run_id] = task
            print 'customer recv %r' % (reply,)
            task.queue.put(reply)
        self.sock = None


class Tunnel(object):
    """A session from the customer to the distributed workers. It can send a
    request of RPC through the customer's sockets.

    :param customer: the :class:`Customer` object.
    :param addrs: the destination worker addresses bound at PULL sockets.
    :param fanout_addrs: the destination worker addresses bound at SUB sockets.
    :param fanout_topic: the filter the workers are subscribing.

    :param wait: if it's set to ``True``, the workers will reply. Otherwise,
                 the workers just invoke a function without reply. Defaults to
                 ``True``.
    :param fanout: if it's set to ``True``, all workers will receive an
                   invocation request. Defaults to ``False``.
    :param as_task: actually, every remote function calls have own
                    :class:`Task` object. if it's set to ``True``, remote
                    functions return a :class:`Task` object instead of result
                    value. Defaults to ``False``.
    """

    def __init__(self, customer, addrs=None, fanout_addrs=None,
                 fanout_topic='', wait=True, fanout=False, as_task=False):
        self._znm_customer = customer
        self._znm_addrs = ensure_sequence(addrs, set)
        self._znm_fanout_addrs = ensure_sequence(fanout_addrs, set)
        self._znm_fanout_topic = fanout_topic
        self._znm_sockets = {}
        # options
        self._znm_wait = wait
        self._znm_fanout = fanout
        self._znm_as_task = as_task

    def _znm_is_alive(self):
        return self in self._znm_customer.tunnels

    def _znm_invoke(self, name, *args, **kwargs):
        """Invokes remote function."""
        customer_addr = list(self._znm_customer.addrs)[0] \
                        if self._znm_wait else None
        task = Task(self)
        sock = self._znm_sockets[zmq.PUB if self._znm_fanout else zmq.PUSH]
        invocation = Invocation(name, args, kwargs, task.id, customer_addr)
        print 'tunnel send %r' % (invocation,)
        topic = self._znm_fanout_topic if self._znm_fanout else ''
        send(sock, invocation, prefix=topic)
        if not self._znm_wait:
            # immediately if workers won't wait
            return
        if not self._znm_customer.running:
            spawn(self._znm_customer.run)
        return task.collect()

    def __getattr__(self, attr):
        return functools.partial(self._znm_invoke, attr)

    def __enter__(self):
        self._znm_customer.register_tunnel(self)
        for socket_type, addrs in [(zmq.PUSH, self._znm_addrs),
                                   (zmq.PUB, self._znm_fanout_addrs)]:
            sock = self._znm_customer.context.socket(socket_type)
            self._znm_sockets[socket_type] = sock
            for addr in addrs:
                sock.connect(addr)
        return self

    def __exit__(self, error, error_type, traceback):
        for sock in self._znm_sockets.viewvalues():
            sock.close()
        self._znm_sockets.clear()
        self._znm_customer.unregister_tunnel(self)

    def __call__(self, wait=None, fanout=None, as_task=None):
        """Creates a :class:`Tunnel` object which follows same consumer and
        workers but replaced options.
        """
        if wait is None:
            wait = self._znm_wait
        if fanout is None:
            fanout = self._znm_fanout
        if as_task is None:
            as_task = self._znm_as_task
        opts = {'wait': wait, 'fanout': fanout, 'as_task': as_task}
        tunnel = Tunnel(self._znm_customer, **opts)
        tunnel._znm_addrs = self._znm_addrs
        tunnel._znm_fanout_addrs = self._znm_fanout_addrs
        tunnel._znm_sockets = self._znm_sockets
        return tunnel


class Task(object):

    def __init__(self, tunnel, id=None, run_id=None):
        self.tunnel = tunnel
        self.customer = tunnel._znm_customer if tunnel is not None else None
        self.id = uuid_str() if id is None else id
        self.run_id = run_id
        self.queue = Queue()

    def collect(self, timeout=0.01):
        assert self.tunnel._znm_wait
        self.customer.register_task(self)
        replies = []
        with Timeout(timeout, False):
            while True:
                reply = self.queue.get()
                assert isinstance(reply, Reply)
                replies.append(reply)
                if not self.tunnel._znm_fanout:
                    break
        self.customer.unregister_task(self)
        if not replies:
            raise AcceptanceError('Failed to find workers that accepted')
        if self.tunnel._znm_fanout:
            tasks = []
            for reply in replies:
                if reply.method == REJECT:
                    # a worker rejected the task
                    continue
                assert reply.method == ACCEPT
                each_task = Task(self.tunnel, self.id, reply.run_id)
                each_task.worker_addr = reply.worker_addr
                tasks.append(each_task)
                self.customer.register_task(each_task)
            return tasks if self.tunnel._znm_as_task else [t() for t in tasks]
        else:
            reply = replies[0]
            assert len(replies) == 1
            assert reply.method == ACCEPT
            self.worker_addr = reply.worker_addr
            self.run_id = reply.run_id
            self.customer.register_task(self)
            return self if self.tunnel._znm_as_task else self()

    def __call__(self):
        reply = self.queue.get()
        print 'task recv %r' % (reply,)
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
            print 'task recv %r' % (reply,)
            assert reply.method not in (ACCEPT, REJECT, RETURN)
            if reply.method == YIELD:
                yield reply.data
            elif reply.method == RAISE:
                raise reply.data
            elif reply.method == BREAK:
                break
