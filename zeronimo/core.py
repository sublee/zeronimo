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
import uuid

from gevent import joinall, spawn, Timeout
from gevent.coros import Semaphore
from gevent.event import AsyncResult
from gevent.queue import Queue, Empty
import zmq.green as zmq

from .exceptions import ZeronimoError, NoWorkerError
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


def st(x):
    return SOCKET_TYPE_NAMES[x]


def dt(x):
    return {1: 'ACK', 2: 'RETURN', 3: 'RAISE', 4: 'YIELD', 5: 'BREAK'}[x]


def generate_inproc_addr():
    return 'inproc://{0}'.format(uuid_str())


def uuid_str():
    import hashlib
    return hashlib.md5(str(uuid.uuid4())).hexdigest()[:6]


class Communicator(object):
    """Manages ZeroMQ sockets."""

    running = 0
    context = None

    def __new__(cls, *args, **kwargs):
        obj = super(Communicator, cls).__new__(cls)
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

    def run(self):
        raise NotImplementedError

    def __del__(self):
        self.running = 0


class Worker(Communicator):

    addrs = None
    fanout_addrs = None
    fanout_filters = None
    functions = None

    def __init__(self, obj, addrs=None, fanout_addrs=None, fanout_filters='',
                 **kwargs):
        if addrs is None:
            addrs = [generate_inproc_addr()]
        if fanout_addrs is None:
            fanout_addrs = [generate_inproc_addr()]
        self.addrs = addrs
        self.fanout_addrs = fanout_addrs
        self.fanout_filters = fanout_filters
        self.functions = collect_remote_functions(obj)
        super(Worker, self).__init__(**kwargs)

    def possible_addrs(self, socket_type):
        if socket_type == zmq.PULL:
            return self.addrs
        elif socket_type == zmq.SUB:
            return self.fanout_addrs
        else:
            socket_type_name = SOCKET_TYPE_NAMES[socket_type]
            raise ValueError('{!r} is not acceptable'.format(socket_type_name))

    def run_task(self, fn, args, kwargs, customer_addr, task_id, waiting):
        run_id = uuid_str()
        print 'worker recv %s%r from %s:%s of %s' % \
              (fn, args, task_id, run_id, customer_addr)
        if waiting:
            sock = self.context.socket(zmq.PUSH)
            sock.connect(customer_addr)
            #TODO: addrs[0] -> public_addr
            sock.send_pyobj((ACK, (self.addrs[0], run_id), task_id, run_id))
        else:
            sock = False
        try:
            val = self.functions[fn](*args, **kwargs)
        except Exception, error:
            print 'worker %s %r to %s:%s' % \
                  (dt(RAISE), error, task_id, run_id)
            sock and sock.send_pyobj((RAISE, error, task_id, run_id))
            raise
        if should_yield(val):
            try:
                for item in val:
                    print 'worker %s %r to %s:%s' % \
                          (dt(YIELD), item, task_id, run_id)
                    sock and sock.send_pyobj((YIELD, item, task_id, run_id))
            except Exception, error:
                print 'worker %s %r to %s:%s' % \
                      (dt(RAISE), error, task_id, run_id)
                sock and sock.send_pyobj((RAISE, error, task_id, run_id))
            else:
                print 'worker %s %r to %s:%s' % \
                      (dt(BREAK), None, task_id, run_id)
                sock and sock.send_pyobj((BREAK, None, task_id, run_id))
        else:
            print 'worker %s %r to %s:%s' % \
                  (dt(RETURN), val, task_id, run_id)
            sock and sock.send_pyobj((RETURN, val, task_id, run_id))

    def run(self):
        self.sock = self.context.socket(zmq.PULL)
        self.fanout_sock = self.context.socket(zmq.SUB)
        self.fanout_sock.setsockopt(zmq.SUBSCRIBE, '')
        # bind addresses
        for addr in self.addrs:
            self.sock.bind(addr)
        for addr in self.fanout_addrs:
            self.fanout_sock.bind(addr)
        # serve both sockets
        def serve(sock):
            while self.running:
                spawn(self.run_task, *sock.recv_pyobj())
        joinall([spawn(serve, self.sock), spawn(serve, self.fanout_sock)])


class Customer(Communicator):

    addr = None
    sock = None
    tunnels = None
    tasks = None

    def __init__(self, addr=None, **kwargs):
        if addr is None:
            addr = 'inproc://{0}'.format(uuid_str())
        self.addr = addr
        self.tunnels = set()
        self.tasks = {}
        self._missing_tasks = {}
        super(Customer, self).__init__(**kwargs)

    def link(self, *args, **kwargs):
        return Tunnel(self, *args, **kwargs)

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
                task.put(*missing.queue.get(block=False))
        except Empty:
            pass

    def run(self):
        assert self.sock is None
        self.sock = self.context.socket(zmq.PULL)
        self.sock.bind(self.addr)
        while self.tunnels:
            try:
                do, val, task_id, run_id = self.sock.recv_pyobj()
            except zmq.ZMQError:
                continue
            if do == ACK:
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
            task.put(do, val)
        self.sock = None


class Tunnel(object):
    """A session from the customer to the distributed workers. It can send a
    request of RPC through the customer's sockets.

    :param customer: the :class:`Customer` object.
    :param workers: the :class:`Worker` objects.
    :param return_task: if set to ``True``, the remote functions return a
                        :class:`Task` object instead of received value.
    :type return_task: bool
    """

    def __init__(self, customer, workers,
                 wait=True, fanout=False, as_task=False):
        self._znm_customer = customer
        self._znm_workers = workers
        self._znm_sockets = {}
        # options
        self._znm_wait = wait
        self._znm_fanout = fanout
        self._znm_as_task = as_task

    def _znm_is_alive(self):
        return self in self._znm_customer.tunnels

    def _znm_invoke(self, fn, *args, **kwargs):
        """Invokes remote function."""
        task = Task(self)
        sock = self._znm_sockets[zmq.PUB if self._znm_fanout else zmq.PUSH]
        sock.send_pyobj((fn, args, kwargs,
                         self._znm_customer.addr, task.id, self._znm_wait))
        if not self._znm_wait:
            # immediately if workers won't wait
            return
        if not self._znm_customer.running:
            spawn(self._znm_customer.run)
        return task.collect()

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
        opts = (wait, fanout, as_task)
        tunnel = Tunnel(self._znm_customer, self._znm_workers, *opts)
        tunnel._znm_sockets = self._znm_sockets
        return tunnel

    def __getattr__(self, attr):
        return functools.partial(self._znm_invoke, attr)

    def __enter__(self):
        self._znm_customer.register_tunnel(self)
        for send_type, recv_type in [PUSH_PULL, PUB_SUB]:
            sock = self._znm_customer.context.socket(send_type)
            for worker in self._znm_workers:
                for addr in worker.possible_addrs(recv_type):
                    sock.connect(addr)
            self._znm_sockets[send_type] = sock
        return self

    def __exit__(self, error, error_type, traceback):
        for sock in self._znm_sockets.viewvalues():
            sock.close()
        self._znm_sockets.clear()
        self._znm_customer.unregister_tunnel(self)


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
        msgs = []
        with Timeout(timeout, False):
            while True:
                msgs.append(self.queue.get())
                if not self.tunnel._znm_fanout:
                    break
        self.customer.unregister_task(self)
        if not msgs:
            raise NoWorkerError('There are no workers which respond')
        if self.tunnel._znm_fanout:
            tasks = []
            for do, (worker_addr, run_id) in msgs:
                assert do == ACK
                each_task = Task(self.tunnel, self.id, run_id)
                each_task.worker_addr = worker_addr
                tasks.append(each_task)
                self.customer.register_task(each_task)
            return tasks if self.tunnel._znm_as_task else [t() for t in tasks]
        else:
            do, val = msgs[0]
            assert len(msgs) == 1
            assert do == ACK
            self.worker_addr, self.run_id = val
            self.customer.register_task(self)
            return self if self.tunnel._znm_as_task else self()

    def put(self, do, val):
        print 'task(%s:%s) recv %s %r' % \
              (self.id, self.run_id, dt(do), val)
        self.queue.put((do, val))

    def __call__(self):
        do, val= self.queue.get()
        if do in (RETURN, RAISE):
            self.customer.unregister_task(self)
        assert do != ACK
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
