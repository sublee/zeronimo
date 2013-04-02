# -*- coding: utf-8 -*-
"""
    zeronimo.core
    ~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import
from contextlib import contextmanager, nested
import functools
from types import MethodType
from uuid import uuid4

from gevent import joinall, spawn
from gevent.coros import Semaphore
from gevent.event import AsyncResult
from gevent.queue import Queue
import zmq.green as zmq

from .functional import collect_remote_functions, should_yield


# task message behaviors
ACK = 1
RETURN = 2
RAISE = 3
YIELD = 4
BREAK = 5


# socket type helpers
SOCKET_TYPE_NAMES = {zmq.REQ: 'REQ', zmq.REP: 'REP', zmq.DEALER: 'DEALER',
                     zmq.ROUTER: 'ROUTER', zmq.PUB: 'PUB', zmq.SUB: 'SUB',
                     zmq.XPUB: 'XPUB', zmq.XSUB: 'XSUB', zmq.PUSH: 'PUSH',
                     zmq.PULL: 'PULL', zmq.PAIR: 'PAIR'}
REQ_REP = (zmq.REQ, zmq.REP)
PUB_SUB = (zmq.PUB, zmq.SUB)
PUSH_PULL = (zmq.PUSH, zmq.PULL)


class Communicator(object):
    """Manages ZeroMQ sockets."""

    running = 0
    context = None

    def __new__(cls, *args, **kwargs):
        obj = super(Communicator, cls).__new__(cls)
        def run(self):
            obj.running += 1
            try:
                obj._actual_run()
            finally:
                obj.running -= 1
                assert obj.running >= 0
        obj.run, obj._actual_run = MethodType(run, obj), obj.run
        return obj

    def __init__(self, context=None):
        self.context = context

    def run(self):
        raise NotImplementedError

    def __del__(self):
        self.running = 0


class Worker(Communicator):

    addr = None
    functions = None

    def __init__(self, obj, addr=None, **kwargs):
        if addr is None:
            addr = 'inproc://{0}'.format(str(uuid4()))
        self.addr = addr
        self.functions = {func.__name__: (func, plan)
                          for func, plan in collect_remote_functions(obj)}
        for func, plan in collect_remote_functions(obj):
            self.functions[func.__name__] = (func, plan)
        super(Worker, self).__init__(**kwargs)

    def possible_addrs(self, socket_type):
        if socket_type == zmq.PULL:
            return [self.addr]
        elif socket_type == zmq.SUB:
            return []
            raise NotImplementedError
        else:
            socket_type_name = SOCKET_TYPE_NAMES[socket_type]
            raise ValueError('{!r} is not acceptable'.format(socket_type_name))

    def run(self):
        joinall([spawn(self._run_direct)])

    def task_received(self, customer_addr, task_id, fn, args, kwargs):
        func, plan = self.functions[fn]
        if plan.reply:
            sock = self.context.socket(zmq.PUSH)
            sock.connect(customer_addr)
            sock.send_pyobj((task_id, ACK, None))
        else:
            sock = False
        try:
            val = func(*args, **kwargs)
        except Exception, error:
            sock and sock.send_pyobj((task_id, RAISE, error))
            raise
        if should_yield(val):
            try:
                for item in val:
                    sock and sock.send_pyobj((task_id, YIELD, item))
            except Exception, error:
                sock and sock.send_pyobj((task_id, RAISE, error))
            else:
                sock and sock.send_pyobj((task_id, BREAK, None))
        else:
            sock and sock.send_pyobj((task_id, RETURN, val))

    def _run_direct(self):
        sock = self.context.socket(zmq.PULL)
        sock.bind(self.addr)
        while self.running:
            spawn(self.task_received, *sock.recv_pyobj())

    def _run_sub(self):
        pass


class Customer(Communicator):

    replies = None

    def __init__(self, addr=None, **kwargs):
        if addr is None:
            addr = 'inproc://{0}'.format(str(uuid4()))
        self.addr = addr
        self.tunnels = set()
        self.lock = Semaphore()
        self.tasks = {}
        super(Customer, self).__init__(**kwargs)

    def link(self, *args, **kwargs):
        return Tunnel(self, *args, **kwargs)

    def run(self):
        if self.lock.locked():
            return
        with self.lock:
            sock = self.context.socket(zmq.PULL)
            sock.bind(self.addr)
            while self.running:
                task_id, do, val = sock.recv_pyobj()
                self.tasks[task_id].put(do, val)

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

    def register_task(self, task):
        self.tasks[task.id] = task

    def unregister_task(self, task):
        assert self.tasks.pop(task.id) is task


class Tunnel(object):
    """A session from the customer to the distributed workers. It can send a
    request of RPC through the customer's sockets.

    :param customer: the :class:`Customer` object.
    :param worker: the :class:`Worker` object.
    :param return_task: if set to ``True``, the remote functions return a
                        :class:`Task` object instead of received value.
    :type return_task: bool
    """

    def __init__(self, customer, worker, return_task=False):
        self._znm_customer = customer
        self._znm_worker = worker
        self._znm_return_task = return_task
        self._znm_sockets = {}
        self._znm_reflect(worker)

    def _znm_invoke(self, fn, *args, **kwargs):
        plan = self._znm_worker.functions[fn][-1]
        task = Task(self)
        sock = self._znm_sockets[zmq.PUB if plan.fanout else zmq.PUSH]
        sock.send_pyobj((self._znm_customer.addr, task.id, fn, args, kwargs))
        if not plan.reply:
            return
        if not self._znm_customer.running:
            spawn(self._znm_customer.run)
        task.prepare()
        return task if self._znm_return_task else task()

    def _znm_reflect(self, worker):
        """Sets methods which follows remote functions with same name."""
        for fn in worker.functions:
            if hasattr(self, fn):
                raise AttributeError('{!r} is already used'.format(fn))
            setattr(self, fn, functools.partial(self._znm_invoke, fn))

    def __enter__(self):
        self._znm_customer.register_tunnel(self)
        for send_type, recv_type in [PUSH_PULL, PUB_SUB]:
            sock = self._znm_customer.context.socket(send_type)
            for addr in self._znm_worker.possible_addrs(recv_type):
                sock.connect(addr)
            self._znm_sockets[send_type] = sock
        return self

    def __exit__(self, error, error_type, traceback):
        for sock in self._znm_sockets.itervalues():
            sock.close()
        self._znm_sockets.clear()
        self._znm_customer.unregister_tunnel(self)


class Task(object):

    def __init__(self, tunnel, id=None):
        self.tunnel = tunnel
        self.customer = tunnel._znm_customer
        self.id = str(uuid4()) if id is None else id
        self.queue = Queue()

    def prepare(self):
        self.customer.register_task(self)
        do, val = self.queue.get()
        assert do == ACK

    def put(self, do, val):
        self.queue.put((do, val))

    def __call__(self):
        do, val= self.queue.get()
        if do in (RETURN, RAISE):
            self.customer.unregister_task(self)
        if do == RETURN:
            return val
        elif do == RAISE:
            raise val
        elif do == YIELD:
            return self.make_generator(val)
        elif do == BREAK:
            return iter([])

    def make_generator(self, first_val):
        yield first_val
        while True:
            do, val = self.queue.get()
            if do == YIELD:
                yield val
            elif do == RAISE:
                raise val
            elif do == BREAK:
                break
