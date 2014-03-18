# -*- coding: utf-8 -*-
"""
    zeronimo.components
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import
from collections import Iterable, Mapping, Sequence, Set
from contextlib import contextmanager
import functools
from types import MethodType
import warnings

from gevent import Greenlet, GreenletExit, Timeout
from gevent.queue import Empty, Queue
try:
    from libuuid import uuid4_bytes
except ImportError:
    import uuid
    uuid4_bytes = lambda: uuid.uuid4().get_bytes()
import zmq.green as zmq

from .exceptions import (
    WorkerNotFound, WorkerNotReachable, TaskRejected,
    SocketClosed, MalformedMessage)
from .helpers import cls_name, make_repr
from .messaging import (
    ACK, DONE, ITER, ACCEPT, REJECT, RETURN, RAISE,
    YIELD, BREAK, PACK, UNPACK, Call, Reply, send, recv)


__all__ = ['Component', 'Worker', 'Customer', 'Collector', 'Task']


# compatible zmq constants
try:
    ZMQ_XPUB = zmq.XPUB
    ZMQ_XSUB = zmq.XSUB
except AttributeError:
    ZMQ_XPUB = -1
    ZMQ_XSUB = -1
try:
    ZMQ_STREAM = zmq.STREAM
except AttributeError:
    ZMQ_STREAM = -1


def is_iterator(obj):
    serializable = (Sequence, Set, Mapping)
    return (isinstance(obj, Iterable) and not isinstance(obj, serializable))


class Component(object):
    """A component should implement :meth:`run`. :attr:`running` is ensured to
    be ``True`` while :meth:`run` is executing.
    """

    greenlet_class = Greenlet

    def __new__(cls, *args, **kwargs):
        obj = super(Component, cls).__new__(cls)
        cls._patch(obj)
        return obj

    @classmethod
    def _patch(cls, obj):
        obj._running = None
        def inner_run(self):
            try:
                cls.run(self)
            except GreenletExit:
                pass
            finally:
                self._running = None
        def run(self):
            self.start()
            return self._running.get()
        obj._inner_run = MethodType(inner_run, obj)
        obj.run = MethodType(run, obj)

    def run(self):
        raise NotImplementedError(
            '{0} has not implementation to run'.format(cls_name(self)))

    def start(self):
        if self.is_running():
            raise RuntimeError('{0} already running'.format(cls_name(self)))
        self._running = self.greenlet_class.spawn(self._inner_run)
        self._running.join(0)
        return self._running

    def stop(self):
        try:
            self._running.kill()
        except AttributeError:
            raise RuntimeError('{0} not running'.format(cls_name(self)))

    def wait(self, timeout=None):
        try:
            self._running.join(timeout)
        except AttributeError:
            raise RuntimeError('{0} not running'.format(cls_name(self)))

    def is_running(self):
        return self._running is not None


class Worker(Component):
    """Worker runs an RPC service of an object through ZeroMQ sockets. The
    ZeroMQ sockets should be PULL or SUB socket type. The PULL sockets receive
    Round-robin calls; the SUB sockets receive Publish-subscribe (fan-out)
    calls.

    .. sourcecode::

       import os
       worker = Worker(os, [sock1, sock2], info='doctor')
       worker.run()

    :param obj: the object to be shared by an RPC service.
    :param sockets: the ZeroMQ sockets of PULL or SUB socket type.
    :param info: (optional) the worker will send this value to customers at
                 accepting an call. it might be identity of the worker to
                 let the customer's know what worker accepted.
    """

    obj = None
    sockets = None
    info = None
    accepting = True

    def __init__(self, obj, sockets, info=None, pack=PACK, unpack=UNPACK):
        super(Worker, self).__init__()
        self.obj = obj
        socket_types = set(s.socket_type for s in sockets)
        if socket_types.difference([zmq.PAIR, zmq.SUB, zmq.PULL, ZMQ_XSUB]):
            raise ValueError('Worker wraps one of PAIR, SUB, PULL, and XSUB')
        self.sockets = sockets
        self.info = info
        self.pack = pack
        self.unpack = unpack
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
                    call = Call(*recv(socket, unpack=self.unpack))
                except BaseException as exc:
                    warning = MalformedMessage(
                        'Received malformed message: {!r}'
                        ''.format(exc.message))
                    warning.message = exc.message
                    warnings.warn(warning)
                    continue
                self.greenlet_class.spawn(self.work, socket, call)

    def work(self, socket, call):
        """Calls a function and send results to the collector. It supports
        all of function actions. A function could return, yield, raise any
        packable objects.
        """
        work_id = uuid4_bytes()
        socket = self.get_reply_socket(socket, call.collector_address)
        channel = (None, None)
        if socket is not None:
            channel = (call.call_id, work_id)
            method = ACCEPT if self.accepting else REJECT
            self.send_reply(socket, method, self.info, *channel)
        if not self.accepting:
            return
        with self.exception_sending(socket, *channel) as raised:
            val = self.call(call)
        if raised():
            return
        if is_iterator(val):
            vals = val
            with self.exception_sending(socket, *channel):
                for val in vals:
                    socket and self.send_reply(socket, YIELD, val, *channel)
                socket and self.send_reply(socket, BREAK, None, *channel)
        else:
            socket and self.send_reply(socket, RETURN, val, *channel)

    @contextmanager
    def exception_sending(self, socket, *channel):
        """Sends an exception which occurs in the context to the collector.
        It raises the caught exception if that was not expected.
        :func:`zeronimo.exceptions.raises` declares expected exception types.
        """
        raised = []
        try:
            yield lambda: bool(raised)
        except BaseException as exc:
            raised.append(True)
            socket and self.send_reply(socket, RAISE, exc, *channel)
            # don't raise at worker-side if the exception was expected
            if not getattr(exc, '_znm_expected', False):
                raise

    def call(self, call):
        """Calls a function."""
        return getattr(self.obj, call.funcname)(*call.args, **call.kwargs)

    def get_reply_socket(self, socket, address):
        if socket.type == zmq.PAIR:
            return socket
        if address is None:
            return
        context = socket.context
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
        # normal tuple is faster than namedtuple
        reply = (method, data, call_id, work_id)
        try:
            return send(socket, reply, zmq.NOBLOCK, pack=self.pack)
        except (zmq.Again, zmq.ZMQError):
            pass

    def __repr__(self):
        keywords = ['info'] if self.info is not None else []
        return make_repr(self, ['obj', 'sockets'], keywords)


class Customer(object):
    """Customer sends RPC calls to the workers. But it could not receive the
    result by itself. It should work with :class:`Collector` to receive
    worker's results.
    """

    _znm_socket = None
    _znm_collector = None
    _znm_topic = None

    def __init__(self, socket, collector=None, topic=None, pack=PACK):
        if socket.type not in [zmq.PAIR, zmq.PUB, zmq.PUSH, ZMQ_XPUB]:
            raise ValueError('Customer wraps one of PAIR, PUB, PUSH, and XPUB')
        self._znm_socket = socket
        self._znm_collector = collector
        self._znm_topic = topic
        self._znm_pack = pack

    def __getitem__(self, topic):
        if self._znm_socket.type not in [zmq.PUB, zmq.XPUB]:
            raise ValueError('Only customer with PUB/XPUB could set a topic')
        cls = type(self)
        customer = cls(self._znm_socket, self._znm_collector)
        customer._znm_topic = topic
        return customer

    def __getattr__(self, attr):
        emit = self._znm_emit if self._znm_collector else self._znm_emit_nowait
        self.__dict__[attr] = functools.partial(emit, attr)
        return self.__dict__[attr]

    def _znm_emit_nowait(self, funcname, *args, **kwargs):
        """Sends a call without call id allocation. It doesn't wait replies."""
        # normal tuple is faster than namedtuple
        call = (funcname, args, kwargs, None, None)
        # use short names
        socket = self._znm_socket
        topic = self._znm_topic
        pack = self._znm_pack
        try:
            send(socket, call, zmq.NOBLOCK, topic, pack)
        except zmq.Again:
            pass  # ignore

    def _znm_emit(self, funcname, *args, **kwargs):
        """Allocates a call id and emit."""
        if not self._znm_collector.is_running():
            self._znm_collector.start()
        call_id = uuid4_bytes()
        collector_address = self._znm_collector.address
        # normal tuple is faster than namedtuple
        call = (funcname, args, kwargs, call_id, collector_address)
        # use short names
        socket = self._znm_socket
        topic = self._znm_topic
        pack = self._znm_pack
        def send_call():
            try:
                send(socket, call, zmq.NOBLOCK, topic, pack)
            except zmq.Again:
                raise WorkerNotReachable('Failed to emit at the moment')
        send_call()
        is_fanout = self._znm_socket.type in [zmq.PUB, zmq.XPUB]
        establish_args = () if is_fanout else (1, send_call)
        try:
            tasks = self._znm_collector.establish(call_id, *establish_args)
        except WorkerNotFound:
            # fanout call returns empty list instead of raising WorkerNotFound
            if is_fanout:
                return []
            else:
                raise
        return tasks if is_fanout else tasks[0]


class Collector(Component):
    """Collector receives results from the worker."""

    def __init__(self, socket, address=None, as_task=False, timeout=0.01,
                 unpack=UNPACK):
        if socket.type not in [zmq.PAIR, zmq.PULL]:
            raise ValueError('Collector wraps PAIR or PULL')
        if address is None and socket.type != zmq.PAIR:
            raise ValueError('Address required')
        if address is not None and socket.type == zmq.PAIR:
            raise ValueError('Address not required when using PAIR socket')
        self.socket = socket
        self.address = address
        self.as_task = as_task
        self.timeout = timeout
        self.unpack = unpack
        self.reply_queues = {}
        self.missing_queues = {}

    def run(self):
        while True:
            try:
                reply = Reply(*recv(self.socket, unpack=self.unpack))
            except GreenletExit:
                break
            except zmq.ZMQError:
                exc = SocketClosed('Collector socket closed')
                self.put_all(Reply(RAISE, exc, None, None))
                break
            except:
                # TODO: warn MalformedMessage
                continue
            try:
                self.dispatch_reply(reply)
            except KeyError:
                # TODO: warning
                continue

    def put_all(self, reply):
        """Puts the reply to all queues."""
        for reply_queues in self.reply_queues.itervalues():
            for reply_queue in reply_queues.itervalues():
                reply_queue.put(reply)

    def dispatch_reply(self, reply):
        """Dispatches the reply to the proper queue."""
        if reply.method & ACK:
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

    def establish(self, call_id, limit=None, retry=None):
        """Waits for the call is accepted by workers and starts to collect the
        tasks.
        """
        accepts = self.wait_accepts(call_id, limit, retry)
        tasks = self.collect_tasks(accepts, call_id, limit)
        return tasks if self.as_task else [task() for task in tasks]

    def wait_accepts(self, call_id, limit=None, retry=None):
        """Waits for the call is accepted by workers. When a worker rejected,
        it calls the retry function to find another worker.
        """
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
                        if retry is not None:
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
            if rejected:
                raise TaskRejected(
                    '{0} workers rejected the task'.format(rejected)
                    if rejected != 1 else 'A worker rejected the task')
            else:
                raise WorkerNotFound('Failed to find worker')
        return accepts

    def collect_tasks(self, accepts, call_id, limit=None):
        """Starts to collect the tasks."""
        tasks = []
        self._collect_more_tasks(tasks, iter(accepts).next, call_id, limit)
        try:
            ack_queue = self.reply_queues[call_id][None]
        except KeyError:
            pass
        else:
            self.greenlet_class.spawn(
                self._collect_more_tasks, tasks, ack_queue.get, call_id, limit)
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
        """Called at the task done for cleaning up the reply queues."""
        reply_queues = self.reply_queues[task.call_id]
        del reply_queues[task.work_id]
        if not reply_queues:
            del self.reply_queues[task.call_id]
            self.missing_queues.pop(task.call_id, None)


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
        assert not reply.method & ACK
        if reply.method & DONE:
            self.collector.task_done(self)
        if reply.method == RETURN:
            return reply.data
        elif reply.method == RAISE:
            raise reply.data
        elif reply.method == YIELD:
            return self.iterator(reply)
        elif reply.method == BREAK:
            return iter([])

    def iterator(self, first_reply):
        """If the method of first reply is YIELD, the result will be a
        generator. This method makes the generator.
        """
        yield first_reply.data
        while True:
            reply = self.reply_queue.get()
            assert not reply.method & ACK and reply.method != RETURN
            if reply.method == YIELD:
                yield reply.data
            elif reply.method == RAISE:
                raise reply.data
            elif reply.method == BREAK:
                break

    def __repr__(self):
        return make_repr(
            self, None, ['call_id', 'work_id', 'worker_info'])
