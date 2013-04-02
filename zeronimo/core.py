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
import types
import uuid

from gevent import joinall, spawn
from gevent.coros import Semaphore
from gevent.event import AsyncResult
from gevent.queue import Queue
import zmq.green as zmq

from .collect import collect_remote_functions


# task message behaviors
ACK = 1
RETURN = 2
RAISE = 3
YIELD = 4


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
        obj.run, obj._actual_run = types.MethodType(run, obj), obj.run
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
    plans = None

    def __init__(self, obj, addr=None, **kwargs):
        if addr is None:
            addr = 'inproc://{0}'.format(str(uuid.uuid4()))
        self.addr = addr
        self.functions = {}
        self.plans = {}
        for func, plan in collect_remote_functions(obj):
            self.functions[func.__name__] = func
            self.plans[func.__name__] = plan
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
        func = self.functions[fn]
        if not self.plans[fn].reply:
            spawn(func, *args, **kwargs)
            return
        sock = self.context.socket(zmq.PUSH)
        sock.connect(customer_addr)
        sock.send_pyobj((task_id, ACK, None))
        value = func(*args, **kwargs)
        sock.send_pyobj((task_id, RETURN, value))

    def _run_direct(self):
        sock = self.context.socket(zmq.PULL)
        sock.bind(self.addr)
        while self.running:
            self.task_received(*sock.recv_pyobj())

    def _run_sub(self):
        pass


class Customer(Communicator):

    replies = None

    def __init__(self, addr=None, **kwargs):
        if addr is None:
            addr = 'inproc://{0}'.format(str(uuid.uuid4()))
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
                task_id, behavior, value = sock.recv_pyobj()
                self.tasks[task_id].put(behavior, value)

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
        plan = self._znm_worker.plans[fn]
        task = Task(self)
        sock = self._znm_sockets[zmq.PUB if plan.fanout else zmq.PUSH]
        sock.send_pyobj((self._znm_customer.addr, task.id, fn, args, kwargs))
        if not plan.reply:
            return
        if not self._znm_customer.running:
            spawn(self._znm_customer.run)
        task.ensure()
        return task if self._znm_return_task else task.get()

    def _znm_reflect(self, worker):
        """Sets methods which follows remote functions with same name."""
        for fn in worker.functions:
            if hasattr(self, fn):
                raise AttributeError('{!r} is already used'.format(fn))
            invoke = functools.partial(self._znm_invoke, fn)
            setattr(self, fn, invoke)

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
        self.id = str(uuid.uuid4()) if id is None else id
        self.queue = Queue()

    def ensure(self):
        self.customer.register_task(self)
        behavior, value = self.queue.get()
        assert behavior == ACK

    def put(self, behavior, value):
        self.queue.put((behavior, value))

    def get(self):
        behavior, value = self.queue.get()
        if behavior == (RETURN, RAISE):
            self.customer.unregister_task(self)
        if behavior == RETURN:
            return value

    def generate(self):
        while True:
            behavior, value = self.customer.recv(self.id)
            if behavior == YIELD:
                yield value
            elif behavior == RAISE:
                raise value
