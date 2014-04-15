# -*- coding: utf-8 -*-
"""
    zeronimo.core
    ~~~~~~~~~~~~~

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import
from collections import Iterable, Mapping, Sequence, Set
from contextlib import contextmanager
import sys
import warnings

from gevent import Greenlet, GreenletExit, Timeout
from gevent.pool import Group
from gevent.queue import Queue
try:
    from libuuid import uuid4_bytes
except ImportError:
    import uuid
    uuid4_bytes = lambda: uuid.uuid4().get_bytes()
import zmq.green as zmq

from .exceptions import (
    EmissionError, WorkerNotFound, Rejected, Undelivered, TaskClosed,
    MalformedMessage)
from .helpers import class_name, socket_type_name
from .messaging import (
    ACK, ACCEPT, REJECT, RETURN, RAISE, YIELD, BREAK, PACK, UNPACK,
    Call, Reply, send, recv)
from .results import RemoteResult


__all__ = ['Worker', 'Customer', 'Fanout', 'Collector']


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


# default timeouts
CUSTOMER_TIMEOUT = 5
FANOUT_TIMEOUT = 0.1


def is_iterator(obj):
    serializable = (Sequence, Set, Mapping)
    return (isinstance(obj, Iterable) and not isinstance(obj, serializable))


class Background(object):
    """A background object spawns one greenlet at a time. The greenlet will
    call :meth:`__call__`.
    """

    #: The greenlet class to be spawned.
    greenlet_class = Greenlet

    #: The current running greenlet.
    greenlet = None

    def __call__(self):
        # should be implemented by subclass.
        raise NotImplementedError(
            '{0} has no __call__ implementation'.format(class_name(self)))

    def run(self):
        try:
            self()
        except GreenletExit:
            pass
        finally:
            del self.greenlet

    def start(self):
        if self.running():
            raise RuntimeError('{0} already running'.format(class_name(self)))
        self.greenlet = self.greenlet_class.spawn(self.run)
        self.greenlet.join(0)
        return self.greenlet

    def stop(self):
        if not self.running():
            raise RuntimeError('{0} not running'.format(class_name(self)))
        self.greenlet.kill(block=True)

    def wait(self, timeout=None):
        if not self.running():
            raise RuntimeError('{0} not running'.format(class_name(self)))
        self.greenlet.join(timeout)

    def running(self):
        return self.greenlet is not None  # and not self.greenlet.dead


class Worker(Background):
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
    greenlet_group = None
    exception_handler = None

    def __init__(self, obj, sockets, info=None, greenlet_group=None,
                 exception_handler=None, pack=PACK, unpack=UNPACK):
        super(Worker, self).__init__()
        self.obj = obj
        socket_types = set(s.type for s in sockets)
        if socket_types.difference([zmq.PAIR, zmq.SUB, zmq.PULL, ZMQ_XSUB]):
            raise ValueError('Worker wraps one of PAIR, SUB, PULL, and XSUB')
        self.sockets = sockets
        self.info = info
        if greenlet_group is None:
            greenlet_group = Group()
        self.greenlet_group = greenlet_group
        self.exception_handler = exception_handler
        self.pack = pack
        self.unpack = unpack
        self._cached_reply_sockets = {}

    def __call__(self):
        """Runs the worker. While running, an RPC service is online."""
        poller = zmq.Poller()
        for socket in self.sockets:
            poller.register(socket, zmq.POLLIN)
        try:
            while True:
                for socket, event in poller.poll():
                    assert event & zmq.POLLIN
                    try:
                        call = Call(*recv(socket, unpack=self.unpack))
                    except BaseException as exc:
                        warning = MalformedMessage(
                            '{0} received malformed message: {1!r}'
                            ''.format(self, exc.message))
                        warning.message = exc.message
                        warnings.warn(warning)
                        continue
                    if self.greenlet_group.full():
                        self.reject(socket, call)
                    else:
                        self.greenlet_group.spawn(self.work, socket, call)
        finally:
            self.greenlet_group.kill()

    def work(self, socket, call):
        """Calls a function and send results to the collector. It supports
        all of function actions. A function could return, yield, raise any
        packable objects.
        """
        channel = (None, None)
        task_id = uuid4_bytes()
        reply_socket = self.get_reply_socket(socket, call.reply_to)
        if reply_socket is not None:
            channel = (call.call_id, task_id)
            self.send_reply(reply_socket, ACCEPT, self.info, *channel)
        with self.exception_sending(reply_socket, *channel) as raised:
            val = self.call(call)
        if raised():
            return
        if is_iterator(val):
            vals = val
            with self.exception_sending(reply_socket, *channel):
                for val in vals:
                    if reply_socket is not None:
                        self.send_reply(reply_socket, YIELD, val, *channel)
                if reply_socket is not None:
                    self.send_reply(reply_socket, BREAK, None, *channel)
        if reply_socket is not None:
            self.send_reply(reply_socket, RETURN, val, *channel)

    def reject(self, socket, call):
        """Sends REJECT reply."""
        reply_socket = self.get_reply_socket(socket, call.reply_to)
        if reply_socket is None:
            return
        self.send_reply(reply_socket, REJECT, self.info, call.call_id, None)

    @contextmanager
    def exception_sending(self, socket, *channel):
        """Sends an exception which occurs in the context to the collector."""
        raised = []
        try:
            yield lambda: bool(raised)
        except BaseException as exc:
            exc_info = sys.exc_info()
            tb = exc_info[-1]
            while tb.tb_next is not None:
                tb = tb.tb_next
            filename = tb.tb_frame.f_code.co_filename
            lineno = tb.tb_lineno
            val = (type(exc), str(exc), filename, lineno)
            try:
                state = exc.__getstate__()
            except AttributeError:
                pass
            else:
                val += (state,)
            socket and self.send_reply(socket, RAISE, val, *channel)
            raised.append(True)
            if self.exception_handler is None:
                raise
            else:
                self.exception_handler(exc_info)

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

    def send_reply(self, socket, method, data, call_id, task_id):
        # normal tuple is faster than namedtuple
        reply = (method, data, call_id, task_id)
        try:
            return send(socket, reply, zmq.NOBLOCK, pack=self.pack)
        except (zmq.Again, zmq.ZMQError):
            pass

    def __repr__(self):
        return '<{0} info={1!r}>'.format(class_name(self), self.info)


class _Emitter(object):

    available_socket_types = NotImplemented
    timeout = NotImplemented

    socket = None
    collector = None
    pack = None

    def __init__(self, socket, collector=None, timeout=None, pack=PACK):
        super(_Emitter, self).__init__()
        if socket.type not in self.available_socket_types:
            raise ValueError('{0} is not available socket type'
                             ''.format(socket_type_name(socket.type)))
        self.socket = socket
        self.collector = collector
        if timeout is not None:
            self.timeout = timeout
        self.pack = pack

    def _emit_nowait(self, funcname, args, kwargs, topic=None):
        call = (funcname, args, kwargs, None, None)
        try:
            send(self.socket, call, zmq.NOBLOCK, topic, self.pack)
        except zmq.Again:
            pass  # ignore

    def _emit(self, funcname, args, kwargs,
              topic=None, limit=None, retry=False):
        """Allocates a call id and emit."""
        if not self.collector.running():
            self.collector.start()
        call_id = uuid4_bytes()
        reply_to = self.collector.address
        # normal tuple is faster than namedtuple
        call = (funcname, args, kwargs, call_id, reply_to)
        # use short names
        def send_call():
            try:
                send(self.socket, call, zmq.NOBLOCK, topic, self.pack)
            except zmq.Again:
                raise Undelivered('Emission was not delivered')
        self.collector.prepare(call_id)
        send_call()
        return self.collector.establish(call_id, self.timeout, limit,
                                        send_call if retry else None)


class Customer(_Emitter):
    """Customer sends RPC calls to the workers. But it could not receive the
    result by itself. It should work with :class:`Collector` to receive
    worker's results.
    """

    available_socket_types = [zmq.PAIR, zmq.PUSH]
    timeout = CUSTOMER_TIMEOUT

    def emit(self, funcname, *args, **kwargs):
        if self.collector is None:
            return self._emit_nowait(funcname, args, kwargs)
        return self._emit(funcname, args, kwargs, limit=1, retry=True)[0]


class Fanout(_Emitter):
    """Customer sends RPC calls to the workers. But it could not receive the
    result by itself. It should work with :class:`Collector` to receive
    worker's results.
    """

    available_socket_types = [zmq.PUB, ZMQ_XPUB]
    timeout = FANOUT_TIMEOUT

    def emit(self, topic, funcname, *args, **kwargs):
        if self.collector is None:
            return self._emit_nowait(funcname, args, kwargs, topic=topic)
        try:
            return self._emit(funcname, args, kwargs, topic=topic)
        except EmissionError:
            return []


class Collector(Background):
    """Collector receives results from the worker."""

    def __init__(self, socket, address=None, unpack=UNPACK):
        super(Collector, self).__init__()
        if socket.type not in [zmq.PAIR, zmq.PULL]:
            raise ValueError('Collector wraps PAIR or PULL')
        if address is None and socket.type != zmq.PAIR:
            raise ValueError('Address required')
        if address is not None and socket.type == zmq.PAIR:
            raise ValueError('Address not required when using PAIR socket')
        self.socket = socket
        self.address = address
        self.unpack = unpack
        self.results = {}
        self.result_queues = {}

    def prepare(self, call_id):
        """"""
        if call_id in self.results:
            raise KeyError('Call {0} already prepared'.format(call_id))
        self.results[call_id] = {}
        self.result_queues[call_id] = Queue()

    def establish(self, call_id, timeout, limit=None, retry=None):
        """Waits for the call is accepted by workers and starts to collect the
        results.
        """
        rejected = 0
        results = []
        result_queue = self.result_queues[call_id]
        try:
            with Timeout(timeout, False):
                while True:
                    result = result_queue.get()
                    if result is None:
                        rejected += 1
                        if retry is not None:
                            retry()
                    else:
                        results.append(result)
                        if limit is not None and len(results) == limit:
                            break
        finally:
            del result_queue
            self.remove_result_queue(call_id)
        if not results:
            if rejected:
                raise Rejected('{0} workers rejected'.format(rejected)
                               if rejected != 1 else 'A worker rejected')
            else:
                raise WorkerNotFound('Failed to find worker')
        return results

    def __call__(self):
        while True:
            try:
                reply = Reply(*recv(self.socket, unpack=self.unpack))
            except GreenletExit:
                break
            except zmq.ZMQError:
                exc = TaskClosed('Collector socket closed')
                for results in self.results.viewvalues():
                    for result in results.viewvalues():
                        result.set_exception(exc)
                break
            except:
                # TODO: warn MalformedMessage
                continue
            try:
                self.dispatch_reply(reply)
            except KeyError:
                # TODO: warning
                continue
            finally:
                del reply

    def dispatch_reply(self, reply):
        """Dispatches the reply to the proper queue."""
        method = reply.method
        call_id = reply.call_id
        task_id = reply.task_id
        if method & ACK:
            try:
                result_queue = self.result_queues[call_id]
            except KeyError:
                raise KeyError('Already established or unprepared call')
            if method == ACCEPT:
                worker_info = reply.data
                result = RemoteResult(self, call_id, task_id, worker_info)
                self.results[call_id][task_id] = result
                result_queue.put_nowait(result)
            elif method == REJECT:
                result_queue.put_nowait(None)
        else:
            result = self.results[call_id][task_id]
            result.set_reply(reply)

    def remove_result(self, result):
        call_id = result.call_id
        task_id = result.task_id
        assert self.results[call_id][task_id] is result
        del self.results[call_id][task_id]
        if call_id not in self.result_queues and not self.results[call_id]:
            del self.results[call_id]

    def remove_result_queue(self, call_id):
        del self.result_queues[call_id]
        if not self.results[call_id]:
            del self.results[call_id]
