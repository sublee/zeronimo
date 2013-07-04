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

import gevent
from gevent import spawn, Timeout
from gevent.coros import Semaphore
from gevent.event import Event, AsyncResult
from gevent.queue import Empty, Queue
from libuuid import uuid4_bytes
import msgpack
import zmq.green as zmq


__version__ = '0.0.dev'
__all__ = ['Worker', 'Customer', 'Collector', 'Task']


# exceptions


class ZeronimoException(Exception):

    pass


class WorkerNotFound(ZeronimoException, LookupError):
    """Occurs by a collector which failed to find any worker accepted within
    the timeout.
    """

    pass


def make_worker_not_found(rejected=0):
    """Generates an error message by the count of workers rejected for
    :exc:`WorkerNotFound`.

        >>> make_worker_not_found(rejected=0)
        WorkerNotFound('Worker not found',)
        >>> make_worker_not_found(rejected=1)
        WorkerNotFound('Worker not found, a worker rejected',)
        >>> make_worker_not_found(rejected=10)
        WorkerNotFound('Worker not found, 10 workers rejected',)
    """
    errmsg = ['Worker not found']
    if rejected == 1:
        errmsg.append('a worker rejected')
    elif rejected:
        errmsg.append('{0} workers rejected'.format(rejected))
    err = WorkerNotFound(', '.join(errmsg))
    err.rejected = rejected
    return err


# utility functions


def should_yield(obj):
    """Returns ``True`` if the object is iterable but not serializable."""
    serializable = (Sequence, Set, Mapping)
    return (isinstance(obj, Iterable) and not isinstance(obj, serializable))


def cls_name(obj):
    """Returns the class name of the object."""
    return type(obj).__name__


def make_repr(obj, params=[], keywords=[], data={}):
    """Generates a string of object initialization code style. It is useful
    for custom __repr__ methods.

        class Example(object):

            def __init__(self, param, keyword=None):
                self.param = param
                self.keyword = keyword

            def __repr__(self):
                return make_repr(self, ['param'], ['keyword'])

        >>> Example('hello', keyword='world')
        Example('hello', keyword='world')
    """
    get = lambda attr: data[attr] if attr in data else getattr(obj, attr)
    opts = []
    if params:
        opts.append(', '.join([repr(get(attr)) for attr in params]))
    if keywords:
        opts.append(', '.join(
            ['{0}={1!r}'.format(attr, get(attr)) for attr in keywords]))
    return '{0}({1})'.format(cls_name(obj), ', '.join(opts))


# message frames


ACCEPT = 1
REJECT = 0
RETURN = 100
RAISE = 101
YIELD = 102
BREAK = 103


_Call = namedtuple('Call', ['function_name', 'args', 'kwargs',
                            'call_id', 'collector_address'])
_Reply = namedtuple('Reply', ['method', 'data', 'call_id', 'work_id'])


class Call(_Call):

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


# transmission of python objects


def default(obj):
    return {'pickle': pickle.dumps(obj)}


def object_hook(obj):
    if 'pickle' in obj:
        return pickle.loads(obj['pickle'])
    return obj


def send(socket, obj, flags=0, topic=None):
    """Same with :meth:`zmq.Socket.send_pyobj` but can append topic for
    filtering subscription.
    """
    serial = msgpack.packb(obj, default=default)
    if topic:
        return socket.send_multipart([topic, serial], flags)
    else:
        return socket.send(serial, flags)


def recv(socket, flags=0):
    """Same with :meth:`zmq.Socket.recv_pyobj`."""
    serial = socket.recv_multipart(flags)[-1]
    return msgpack.unpackb(serial, object_hook=object_hook)


# components


class Runner(object):
    """A runner should implement :meth:`run`. :attr:`running` is ensured to be
    ``True`` while :meth:`run` is runnig.
    """

    def __new__(cls, *args, **kwargs):
        obj = super(Runner, cls).__new__(cls)
        cls._patch(obj)
        return obj

    @classmethod
    def _patch(cls, obj):
        obj._running = None
        def run_and_clean(self):
            try:
                cls.run(self)
            except gevent.GreenletExit:
                pass
            finally:
                self._running = None
        def start(self):
            if self.is_running():
                raise RuntimeError('{0} already running'.format(cls_name(self)))
            self._running = spawn(run_and_clean, self)
        def run(self):
            self.start()
            return self._running.get()
        obj.start, obj.run = MethodType(start, obj), MethodType(run, obj)

    def run(self):
        raise NotImplementedError(
            '{0} has not implementation to run'.format(cls_name(self)))

    def start(self):
        pass

    def stop(self):
        try:
            self._running.kill()
        except AttributeError:
            raise RuntimeError('{0} not running'.format(cls_name(self)))

    def wait(self, timeout=None):
        try:
            self._running.wait(timeout)
        except AttributeError:
            raise RuntimeError('{0} not running'.format(cls_name(self)))

    def is_running(self):
        return self._running is not None


class Worker(Runner):
    """The worker object runs an RPC service of an object through ZMQ sockets.
    The ZMQ sockets should be PULL or SUB socket type. The PULL sockets receive
    Round-robin calls; the SUB sockets receive Publish-subscribe
    (fan-out) calls.

    ::

       import os
       worker = Worker(os, [sock1, sock2], info='doctor')
       worker.run()

    :param obj: the object to be shared by an RPC service.
    :param sockets: the ZMQ sockets of PULL or SUB socket type.
    :param info: (optional) the worker will send this value to customers at
                 accepting an call. it might be identity of the worker to
                 let the customer's know what worker accepted.
    """

    obj = None
    sockets = None
    info = None
    accepting = True

    def __init__(self, obj, sockets, info=None):
        super(Worker, self).__init__()
        self.obj = obj
        if set(s.socket_type for s in sockets).difference([zmq.PULL, zmq.SUB]):
            raise ValueError('Worker socket should be PULL or SUB')
        self.sockets = sockets
        self.info = info
        self._cached_reply_sockets = {}

    def accept_all(self):
        """After calling this, the worker will accept all calls. This
        will be called at the initialization of the worker.
        """
        self.accepting = True

    def reject_all(self):
        """After calling this, the worker will reject all calls. If the
        worker is busy, it will be helpful.
        """
        self.accepting = False

    def run(self):
        """Runs the worker. While running, an RPC service is online."""
        poller = zmq.Poller()
        for socket in self.sockets:
            poller.register(socket, zmq.POLLIN)
        while True:
            for socket, event in poller.poll():
                assert event & zmq.POLLIN
                try:
                    call = Call(*recv(socket))
                except (TypeError, msgpack.ExtraData):
                    # TODO: warning
                    continue
                spawn(self.work, call, socket.context)

    def call(self, call):
        return getattr(self.obj, call.function_name)(*call.args, **call.kwargs)

    def work(self, call, context):
        """Invokes a function and send results to the customer. It supports
        all of function actions. A function could return, yield, raise any
        picklable objects.
        """
        work_id = uuid4_bytes()
        socket = None
        if call.collector_address is not None:
            socket = self.get_reply_socket(call.collector_address, context)
            channel = (call.call_id, work_id)
            method = ACCEPT if self.accepting else REJECT
            self.send_reply(socket, method, self.info, *channel)
        if not self.accepting:
            return
        try:
            val = self.call(call)
        except Exception as error:
            # raise
            socket and self.send_reply(socket, RAISE, error, *channel)
            raise
        if should_yield(val):
            # yield, yield, ..., break
            try:
                for item in val:
                    socket and self.send_reply(socket, YIELD, item, *channel)
            except Exception as error:
                socket and self.send_reply(socket, RAISE, error, *channel)
            else:
                socket and self.send_reply(socket, BREAK, None, *channel)
        else:
            # return
            socket and self.send_reply(socket, RETURN, val, *channel)

    def get_reply_socket(self, address, context):
        try:
            sockets = self._cached_reply_sockets[context]
        except KeyError:
            sockets = {}
            self._cached_reply_sockets[context] = sockets
        try:
            return sockets[address]
        except KeyError:
            socket = context.socket(zmq.PUSH)
            socket.connect(address)
            sockets[address] = socket
            return socket

    def send_reply(self, socket, method, data, call_id, work_id):
        reply = Reply(method, data, call_id, work_id)
        return send(socket, reply)

    def __repr__(self):
        keywords = ['info'] if self.info is not None else []
        return make_repr(self, ['obj', 'sockets'], keywords)


class Customer(object):

    _znm_socket = None
    _znm_collector = None
    _znm_topic = None

    def __init__(self, socket, collector=None):
        if socket.type not in (zmq.PUSH, zmq.PUB):
            raise ValueError('Customer socket should be PUSH or PUB')
        self._znm_socket = socket
        self._znm_collector = collector

    def __getitem__(self, topic):
        cls = type(self)
        customer = cls(self._znm_socket, self._znm_collector)
        customer._znm_topic = topic
        return customer

    def __getattr__(self, attr):
        emit = self._znm_emit if self._znm_collector else self._znm_emit_nowait
        self.__dict__[attr] = functools.partial(emit, attr)
        return self.__dict__[attr]

    def _znm_emit_nowait(self, function_name, *args, **kwargs):
        """Sends a call without call id allocation. It doesn't wait replies."""
        # normal tuple is faster than namedtuple
        call = (function_name, args, kwargs, None, None)
        send(self._znm_socket, call, topic=self._znm_topic)

    def _znm_emit(self, function_name, *args, **kwargs):
        """Allocates a call id and emit."""
        call_id = uuid4_bytes()
        collector_address = self._znm_collector.address
        # normal tuple is faster than namedtuple
        call = (function_name, args, kwargs, call_id, collector_address)
        send_call = functools.partial(send, self._znm_socket, call,
                                      topic=self._znm_topic)
        send_call()
        if not self._znm_collector.is_running():
            self._znm_collector.start()
        limit = None if self._znm_socket.type == zmq.PUB else 1
        tasks = self._znm_collector.establish(call_id, send_call, limit)
        return tasks[0] if limit == 1 else tasks


class Collector(Runner):

    def __init__(self, socket, address, timeout=0.01, as_task=False):
        if socket.type != zmq.PULL:
            raise ValueError('Collector socket should be PULL')
        self.socket = socket
        self.address = address
        self.timeout = timeout
        self.as_task = as_task
        self.reply_queues = {}
        self.missing_queues = {}

    def run(self):
        while True:
            try:
                reply = Reply(*recv(self.socket))
            except (TypeError, msgpack.ExtraData):
                # TODO: warning
                continue
            self.dispatch_reply(reply)

    def dispatch_reply(self, reply):
        if reply.method in (ACCEPT, REJECT):
            self.reply_queues[reply.call_id][None].put(reply)
            return
        reply_queues = self.reply_queues[reply.call_id]
        try:
            reply_queue = reply_queues[reply.work_id]
        except KeyError:
            try:
                missing_queues = self.missing_queues[reply.call_id]
            except KeyError:
                missing_queues = {}
                self.missing_queues[reply.call_id] = missing_queues
            reply_queue = Queue()
            try:
                reply_queue = missing_queues[reply.work_id]
            except KeyError:
                reply_queue = Queue()
                missing_queues[reply.work_id] = reply_queue
        reply_queue.put(reply)

    def establish(self, call_id, retry, limit=None):
        accepts = self.wait_accepts(call_id, limit)
        tasks = self.collect_tasks(accepts, call_id, limit)
        return tasks if self.as_task else [task() for task in tasks]

    def wait_accepts(self, call_id, limit=None):
        ack_queue = Queue()
        self.reply_queues[call_id] = {None: ack_queue}
        accepts = []
        rejected = 0
        try:
            with Timeout(self.timeout, False):
                while True:
                    reply = ack_queue.get()
                    if reply.method == REJECT:
                        rejected += 1
                        retry()
                        continue
                    elif reply.method == ACCEPT:
                        accepts.append(reply)
                        if limit is None:
                            continue
                        elif len(accepts) == limit:
                            break
        finally:
            if limit is not None:
                del self.reply_queues[call_id][None]
        if not accepts:
            del self.reply_queues[call_id]
            raise make_worker_not_found(rejected)
        return accepts

    def collect_tasks(self, accepts, call_id, limit=None):
        tasks = []
        self._collect_more_tasks(tasks, iter(accepts).next, call_id, limit)
        try:
            ack_queue = self.reply_queues[call_id][None]
        except KeyError:
            pass
        else:
            spawn(self._collect_more_tasks,
                  tasks, ack_queue.get, call_id, limit)
        return tasks

    def _collect_more_tasks(self, tasks, get_reply, call_id, limit=None):
        iterator = iter(get_reply, None)
        while limit is None or len(tasks) < limit:
            try:
                reply = next(iterator)
            except StopIteration:
                break
            if reply is StopIteration:
                break
            assert reply.method == ACCEPT
            assert reply.call_id == call_id
            reply_queue = Queue()
            work_id = reply.work_id
            worker_info = reply.data
            self.reply_queues[call_id][work_id] = reply_queue
            # recover missing replies
            try:
                missing_queue = self.missing_queues[call_id].pop(work_id)
            except KeyError:
                pass
            else:
                try:
                    while True:
                        reply_queue.put(missing_queue.get(block=False))
                except Empty:
                    pass
            task = Task(self, reply_queue, call_id, work_id, worker_info)
            tasks.append(task)

    def task_done(self, task):
        reply_queues = self.reply_queues[task.call_id]
        del reply_queues[task.work_id]
        if not reply_queues:
            del self.reply_queues[task.call_id]


class Task(object):
    """The task object.

    :param customer: the customer object.
    :param id: the task identifier.
    :param invoker_id: the identifier of the invoker which spawned this task.
    :param worker_info: the value the worker sent at accepting.
    """

    def __init__(self, collector, reply_queue, call_id, work_id,
                 worker_info=None):
        self.collector = collector
        self.reply_queue = reply_queue
        self.call_id = call_id
        self.work_id = work_id
        self.worker_info = worker_info

    def __call__(self):
        """Gets the result."""
        reply = self.reply_queue.get()
        assert reply.method not in (ACCEPT, REJECT)
        if reply.method in (RETURN, RAISE):
            self.collector.task_done(self)
        if reply.method == RETURN:
            return reply.data
        elif reply.method == RAISE:
            raise reply.data
        elif reply.method == YIELD:
            return self.itertor(reply)
        elif reply.method == BREAK:
            return iter([])

    def iterator(self, first_reply):
        yield first_reply.data
        while True:
            reply = self.reply_queue.get()
            assert reply.method not in (ACCEPT, REJECT, RETURN)
            if reply.method == YIELD:
                yield reply.data
            elif reply.method == RAISE:
                raise reply.data
            elif reply.method == BREAK:
                break

    def __repr__(self):
        return make_repr(
            self, None, ['call_id', 'work_id', 'worker_info'])
