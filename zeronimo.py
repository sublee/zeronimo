# -*- coding: utf-8 -*-
"""
    zeronimo
    ~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import namedtuple, Iterable, Sequence, Set, Mapping
import functools
try:
    import cPickle as pickle
except ImportError:
    import pickle
from types import MethodType

from gevent import joinall, spawn, Timeout
from gevent.coros import Semaphore
from gevent.event import Event
from gevent.queue import Queue, Empty
import zmq.green as zmq


__version__ = '0.0.dev'
__all__ = []


# exceptions


class ZeronimoError(Exception): pass


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
                  100: 'RETURN', 101: 'RAISE',
                  102: 'YIELD', 103: 'BREAK'}[self.method]
        args = (type(self).__name__, method) + self[1:]
        return '{}({}, {!r}, {!r}, {!r}, {!r})'.format(*args)


# reply methods


ACCEPT = 1
REJECT = 0
RETURN = 100
RAISE = 101
YIELD = 102
BREAK = 103


# utility functions


def alloc_id():
    import hashlib
    import uuid
    return hashlib.md5(str(uuid.uuid4())).hexdigest()[:6]


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

    def run(self, should_run):
        raise NotImplementedError

    def stop(self):
        self.running_lock.wait()


class ZMQSocketManager(object):
    """Manages ZeroMQ sockets."""

    context = None
    sock = None
    addrs = None

    def __init__(self, context=None):
        if context is None:
            context = zmq.Context()
        self.context = context
        self.addrs = set()

    def open_sockets(self):
        """Opens all sockets if not opened."""
        if self.sock is not None:
            return
        self.sock = self.context.socket(zmq.PULL)
        for addr in self.addrs:
            self.bind(addr)

    def close_sockets(self):
        """Closes all sockets if not closed."""
        if self.sock is None:
            return
        self.sock.close()
        self.sock = None

    def bind(self, addr):
        """Binds an address to the socket."""
        if self.sock is not None:
            self.sock.bind(addr)
        self.addrs.add(addr)

    def unbind(self, addr):
        """Unbinds an address to the socket."""
        if self.sock is not None:
            self.sock.unbind(addr)
        self.addrs.remove(addr)


class Worker(Runner, ZMQSocketManager):

    obj = None
    fanout_sock = None
    fanout_addrs = None
    fanout_topic = None
    accepting = True

    def __init__(self, obj, bind=None, bind_fanout=None, fanout_topic='',
                 **kwargs):
        super(Worker, self).__init__(**kwargs)
        self.obj = obj
        self.fanout_addrs = set()
        bind and self.bind(bind)
        bind_fanout and self.bind_fanout(bind_fanout)
        self.subscribe(fanout_topic)

    def open_sockets(self):
        super(Worker, self).open_sockets()
        if self.fanout_sock is not None:
            return
        self.fanout_sock = self.context.socket(zmq.SUB)
        for addr in self.fanout_addrs:
            self.bind_fanout(addr)
        if self.fanout_topic is not None:
            self.subscribe(self.fanout_topic)

    def close_sockets(self):
        super(Worker, self).close_sockets()
        if self.fanout_sock is None:
            return
        self.fanout_sock.close()
        self.fanout_sock = None

    def bind_fanout(self, addr):
        """Binds the address to SUB socket."""
        if self.fanout_sock is not None:
            self.fanout_sock.bind(addr)
        self.fanout_addrs.add(addr)

    def unbind_fanout(self, addr):
        """Unbinds the address to SUB socket."""
        if self.fanout_sock is not None:
            self.fanout_sock.unbind(addr)
        self.fanout_addrs.remove(addr)

    def subscribe(self, fanout_topic):
        if self.fanout_sock:
            if self.fanout_topic is not None:
                self.fanout_sock.setsockopt(zmq.UNSUBSCRIBE, self.fanout_topic)
            self.fanout_sock.setsockopt(zmq.SUBSCRIBE, fanout_topic)
        self.fanout_topic = fanout_topic

    def accept_all(self):
        self.accepting = True

    def reject_all(self):
        self.accepting = False

    def run_task(self, invocation):
        run_id = alloc_id()
        #print 'worker recv', invocation
        meta = (invocation.task_id, run_id, list(self.addrs)[0])
        name = invocation.name
        args = invocation.args
        kwargs = invocation.kwargs
        if invocation.customer_addr is None:
            sock = False
        else:
            sock = self.context.socket(zmq.PUSH)
            sock.connect(invocation.customer_addr)
            method = ACCEPT if self.accepting else REJECT
            #print 'worker send %r' % (Reply(method, None, *meta),)
            zmq_send(sock, Reply(method, None, *meta))
        if not self.accepting:
            #print 'task rejected'
            return
        try:
            val = getattr(self.obj, name)(*args, **kwargs)
        except Exception as error:
            #print 'worker send %r' % (Reply(RAISE, error, *meta),)
            sock and zmq_send(sock, Reply(RAISE, error, *meta))
            raise
        if should_yield(val):
            try:
                for item in val:
                    #print 'worker send %r' % (Reply(YIELD, item, *meta),)
                    sock and zmq_send(sock, Reply(YIELD, item, *meta))
            except Exception as error:
                #print 'worker send %r' % (Reply(RAISE, error, *meta),)
                sock and zmq_send(sock, Reply(RAISE, error, *meta))
            else:
                #print 'worker send %r' % (Reply(BREAK, None, *meta),)
                sock and zmq_send(sock, Reply(BREAK, None, *meta))
        else:
            #print 'worker send %r' % (Reply(RETURN, val, *meta),)
            sock and zmq_send(sock, Reply(RETURN, val, *meta))

    def run(self, should_run):
        def serve(sock, prefix=''):
            while should_run():
                try:
                    spawn(self.run_task, zmq_recv(sock, prefix=prefix))
                except zmq.ZMQError:
                    continue
        self.open_sockets()
        try:
            joinall([spawn(serve, self.sock),
                     spawn(serve, self.fanout_sock, self.fanout_topic)])
        finally:
            self.close_sockets()

    def stop(self):
        self.close_sockets()
        super(Worker, self).stop()

    def __repr__(self):
        return '{0}({1}, {2}, {3})'.format(type(self).__name__, self.addrs,
                                           self.fanout_addrs, self.fanout_topic)


class Customer(Runner, ZMQSocketManager):

    tunnels = None
    tasks = None

    def __init__(self, bind=None, reuse=False, **kwargs):
        super(Customer, self).__init__(**kwargs)
        self.tunnels = set()
        self.tasks = {}
        self._missing_tasks = {}
        bind and self.bind(bind)
        self.reuse = reuse

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
        self.open_sockets()

    def unregister_tunnel(self, tunnel):
        """Unregisters the :class:`Tunnel` object."""
        self.tunnels.remove(tunnel)
        if not self.reuse and self.is_running() and not self.tunnels:
            self.stop()

    def register_task(self, task):
        try:
            self.tasks[task.id][task.run_id] = task
        except KeyError:
            self.tasks[task.id] = {task.run_id: task}
        self._restore_missing_messages(task)
        if not self.is_running():
            spawn(self.run)

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

    def run(self, should_run):
        while should_run():
            try:
                reply = zmq_recv(self.sock)
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
            #print 'customer recv %r' % (reply,)
            task.queue.put(reply)

    def stop(self):
        self.close_sockets()
        super(Customer, self).stop()

    def __repr__(self):
        return '{0}({1})'.format(type(self).__name__, self.addrs)


class Tunnel(object):
    """A session between the customer and the distributed workers. It can send
    a request of RPC through sockets on the customer's context.

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
    :param timeout: the seconds to timeout for collecting workers which
                    accepted a task. Defaults to 0.01 seconds.
    """

    __opts__ = {'wait': True, 'fanout': False, 'as_task': False}

    def __init__(self, customer, addrs=None, fanout_addrs=None,
                 fanout_topic='', **opts):
        self._znm_customer = customer
        self._znm_addrs = ensure_sequence(addrs, set)
        self._znm_fanout_addrs = ensure_sequence(fanout_addrs, set)
        self._znm_fanout_topic = fanout_topic
        self._znm_sockets = {}
        # options
        self._znm_opts = {}
        self._znm_opts.update(self.__opts__)
        self._znm_opts.update(opts)

    def _znm_invoke(self, name, *args, **kwargs):
        """Invokes remote function."""
        opts = self._znm_opts
        task = Task(self)
        sock = self._znm_sockets[zmq.PUB if opts['fanout'] else zmq.PUSH]
        if opts['wait']:
            #TODO: addr negotiation
            customer_addr = list(self._znm_customer.addrs)[0]
        else:
            customer_addr = None
        invocation = Invocation(name, args, kwargs, task.id, customer_addr)
        fanout_topic = self._znm_fanout_topic if opts['fanout'] else ''
        #print 'tunnel send %r upon %r' % (invocation, fanout_topic)
        return task.invoke(name, args, kwargs)

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

    def __call__(self, **replacing_opts):
        """Creates a :class:`Tunnel` object which follows same consumer and
        workers but replaced options.
        """
        opts = {}
        opts.update(self._znm_opts)
        opts.update(replacing_opts)
        tunnel = Tunnel(self._znm_customer, self._znm_addrs,
                        self._znm_fanout_addrs, self._znm_fanout_topic,
                        **opts)
        tunnel._znm_sockets = self._znm_sockets
        return tunnel


class Task(object):

    def __init__(self, tunnel, id=None, run_id=None):
        self.tunnel = tunnel
        self.customer = tunnel._znm_customer if tunnel is not None else None
        self.id = alloc_id() if id is None else id
        self.run_id = run_id
        self.queue = Queue()

    def invoke(self, name, args, kwargs, timeout=0.01):
        wait = self.tunnel._znm_opts['wait']
        fanout = self.tunnel._znm_opts['fanout']
        if wait:
            #TODO: addr negotiation
            customer_addr = list(self.tunnel._znm_customer.addrs)[0]
        else:
            customer_addr = None
        invocation = Invocation(name, args, kwargs, self.id, customer_addr)
        sock = self.tunnel._znm_sockets[zmq.PUB if fanout else zmq.PUSH]
        # send the invocation
        if fanout:
            zmq_send(sock, invocation, prefix=self.tunnel._znm_fanout_topic)
        else:
            zmq_send(sock, invocation)
        if not wait:
            return  # don't wait for results
        acceptances = []
        self.customer.register_task(self)
        with Timeout(timeout, False):
            while True:
                reply = self.queue.get()
                assert isinstance(reply, Reply)
                if reply.method == REJECT:
                    if not fanout:
                        zmq_send(sock, invocation)  # re-send
                    continue
                assert reply.method == ACCEPT
                acceptances.append(reply)
                if not fanout:
                    break
        self.customer.unregister_task(self)
        if not acceptances:
            raise ZeronimoError('Failed to find workers that accepted')
        if fanout:
            return self.spawn_fanout_tasks(acceptances)
        else:
            assert len(acceptances) == 1
            return self.spawn_task(acceptances[0])

    def spawn_task(self, acceptance):
        as_task = self.tunnel._znm_opts['as_task']
        self.worker_addr = acceptance.worker_addr
        self.run_id = acceptance.run_id
        self.customer.register_task(self)
        return self if as_task else self()

    def spawn_fanout_tasks(self, acceptances):
        as_task = self.tunnel._znm_opts['as_task']
        tasks = []
        for acceptance in acceptances:
            task = type(self)(self.tunnel, self.id, acceptance.run_id)
            task.worker_addr = acceptance.worker_addr
            tasks.append(task)
            self.customer.register_task(task)
        return tasks if as_task else [t() for t in tasks]

    def __call__(self):
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
