# -*- coding: utf-8 -*-
"""
   zeronimo.core
   ~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from collections import Iterator
from contextlib import contextmanager
from functools import partial
import sys
import traceback
from warnings import warn

from gevent import Greenlet, GreenletExit, Timeout
from gevent.pool import Group
from gevent.queue import Queue
try:
    from libuuid import uuid4_bytes
except ImportError:
    import uuid
    uuid4_bytes = lambda: uuid.uuid4().get_bytes()
import zmq.green as zmq

from .application import default_rpc_spec, rpc_table
from .exceptions import (
    EmissionError, MalformedMessage, Reject, Rejected, TaskClosed, Undelivered,
    WorkerNotFound)
from .helpers import class_name, eintr_retry_zmq, Flag, socket_type_name
from .messaging import (
    ACCEPT, ACK, BREAK, Call, PACK, RAISE, recv, REJECT, Reply, RETURN, send,
    UNPACK, YIELD)
from .results import RemoteResult


__all__ = ['Worker', 'Customer', 'Fanout', 'Collector']


# Compatible zmq constants.
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


# Default timeouts.
CUSTOMER_TIMEOUT = 5
FANOUT_TIMEOUT = 0.1


# Used for a value for `reply_to`.  When a worker receives this, the worker
# should reply through the socket received the call.
Duplex = object()


class Background(object):
    """A background object spawns one greenlet at a time.  The greenlet will
    call :meth:`__call__`.
    """

    #: The greenlet class to be spawned.
    greenlet_class = Greenlet

    #: The current running greenlet.
    greenlet = None

    def __call__(self):
        # should be implemented by subclass.
        raise NotImplementedError('{0} has no __call__ implementation'
                                  ''.format(class_name(self)))

    def run(self):
        try:
            self()
        except GreenletExit:
            pass
        finally:
            del self.greenlet

    def start(self, silent=False):
        if self.is_running():
            if silent:
                return
            raise RuntimeError('{0} already running'.format(class_name(self)))
        self.greenlet = self.greenlet_class.spawn(self.run)
        self.greenlet.join(0)
        return self.greenlet

    def stop(self, silent=False):
        if not self.is_running():
            if silent:
                return
            raise RuntimeError('{0} not running'.format(class_name(self)))
        self.greenlet.kill(block=True)

    def wait(self, timeout=None):
        if not self.is_running():
            raise RuntimeError('{0} not running'.format(class_name(self)))
        self.greenlet.join(timeout)

    def is_running(self):
        return self.greenlet is not None

    def running(self):
        warn(DeprecationWarning('use is_running() instead'))
        return self.is_running()

    def close(self):
        self.stop(silent=True)


def default_exception_handler(worker, exc_info):
    """The default exception handler for :class:`Worker`.  It just raises
    the given ``exc_info``.
    """
    raise exc_info[0], exc_info[1], exc_info[2]


def default_malformed_message_handler(worker, exc_info, message_parts):
    """The default malformed message handler for :class:`Worker`.  It warns
    as a :exc:`MalformedMessage`.
    """
    exc_type, exc, tb = exc_info
    exc_strs = traceback.format_exception_only(exc_type, exc)
    exc_str = exc_strs[0].strip()
    if len(exc_strs) > 1:
        exc_str += '...'
    warn('<%s> occurred by %r' % (exc_str, message_parts), MalformedMessage)


def _ack(worker, reply_socket, channel, call, acked,
         accept=True, silent=False):
    if not reply_socket:
        return
    elif acked:
        if silent:
            return
        raise RuntimeError('already acknowledged')
    acked(True)
    if accept:
        worker.accept(reply_socket, channel)
    else:
        worker.reject(reply_socket, call)
        if not silent:
            raise Reject


def verify_socket_types(name, available_socket_types, *sockets):
    if not set(s.type for s in sockets).difference(available_socket_types):
        return
    only = ', '.join(socket_type_name(t) for t in available_socket_types)
    raise ValueError('%s accepts only %s' % (name, only))


class Worker(Background):
    """Worker runs an RPC service of an object through ZeroMQ sockets.  The
    ZeroMQ sockets should be PULL or SUB socket type.  The PULL sockets receive
    Round-robin calls; the SUB sockets receive Publish-subscribe (fan-out)
    calls.

    .. sourcecode::

       import os
       worker = Worker(os, [sock1, sock2], info='doctor')
       worker.run()

    :param app: the application to be shared by an RPC service.
    :param sockets: the ZeroMQ sockets of PULL or SUB socket type.
    :param info: (optional) the worker will send this value to customers at
                 accepting an call.  it might be identity of the worker to
                 let the customer's know what worker accepted.
    """

    worker_sockets = None
    reply_socket = None
    info = None
    greenlet_group = None
    exception_handler = None
    malformed_message_handler = None
    cache_factory = None
    pack = None
    unpack = None

    def __init__(self, app, worker_sockets, reply_socket=None,
                 info=None, greenlet_group=None,
                 exception_handler=default_exception_handler,
                 malformed_message_handler=default_malformed_message_handler,
                 cache_factory=dict, pack=PACK, unpack=UNPACK):
        super(Worker, self).__init__()
        self.app = app
        verify_socket_types(self.__class__.__name__, [
            zmq.PAIR, zmq.ROUTER, zmq.PULL, zmq.SUB, ZMQ_XSUB
        ], *worker_sockets)
        self.worker_sockets = worker_sockets
        self.reply_socket = reply_socket
        self.info = info
        if greenlet_group is None:
            greenlet_group = Group()
        self.greenlet_group = greenlet_group
        self.exception_handler = exception_handler
        self.malformed_message_handler = malformed_message_handler
        self.cache_factory = cache_factory
        self.pack = pack
        self.unpack = unpack
        self._cached_reply_sockets = {}

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = app
        self.rpc_table = rpc_table(app)

    @app.deleter
    def app(self):
        del self._app
        del self.rpc_table

    @property
    def obj(self):
        warn('use app instead', DeprecationWarning)
        return self.app

    def __call__(self):
        """Runs the worker.  While running, an RPC service is online."""
        poller = zmq.Poller()
        for socket in self.worker_sockets:
            poller.register(socket, zmq.POLLIN)
        try:
            while True:
                for socket, event in eintr_retry_zmq(poller.poll):
                    assert event & zmq.POLLIN
                    try:
                        prefix, args = recv(socket, unpack=self.unpack)
                    except MalformedMessage as exc:
                        # The worker received a malformed message.
                        if self.malformed_message_handler is not None:
                            __, __, tb = sys.exc_info()
                            inner_exc = exc.exception
                            msg_parts = exc.message_parts
                            exc_info = (inner_exc.__class__, inner_exc, tb)
                            handle = self.malformed_message_handler
                            handle(self, exc_info, msg_parts)
                        continue
                    call = Call(*args)
                    if self.greenlet_group.full():
                        # Reject immediately.
                        reply_socket, prefix = \
                            self.get_replier(socket, prefix, call.reply_to)
                        self.reject(reply_socket, call, prefix)
                        continue
                    self.greenlet_group.spawn(self.work, socket, call, prefix)
                    self.greenlet_group.join(0)
        finally:
            self.greenlet_group.kill()
            for sockets in self._cached_reply_sockets.viewvalues():
                for socket in sockets.values():
                    socket.close()
            self._cached_reply_sockets.clear()

    def work(self, socket, call, prefix=None):
        """Calls a function and send results to the collector.  It supports
        all of function actions.  A function could return, yield, raise any
        packable objects.
        """
        task_id = uuid4_bytes()
        reply_socket, prefix = self.get_replier(socket, prefix, call.reply_to)
        if reply_socket:
            channel = (call.call_id, task_id, prefix)
        else:
            channel = (None, None, None)
        f, rpc_spec = self.find_call_target(call)
        acked = Flag()
        ack = partial(_ack, self, reply_socket, channel, call, acked)
        if not rpc_spec.manual_ack:
            # Acknowledge automatically.
            ack()
        success = False
        with self.catch_exceptions():
            try:
                val = self.call(call, ack, f, rpc_spec)
            except Reject:
                return
            except:
                exc_info = sys.exc_info()
                ack(accept=False, silent=True)
                self.raise_(reply_socket, channel, exc_info)
                raise exc_info[0], exc_info[1], exc_info[2]
            success = True
        if not success:
            # catch_exceptions() hides exceptions.
            return
        if isinstance(val, Iterator):
            vals = val
            with self.catch_exceptions():
                try:
                    try:
                        val = next(vals)
                    except StopIteration:
                        ack(accept=True, silent=True)
                    else:
                        ack(accept=True, silent=True)
                        self.send_reply(reply_socket, YIELD, val, *channel)
                        for val in vals:
                            self.send_reply(reply_socket, YIELD, val, *channel)
                    self.send_reply(reply_socket, BREAK, None, *channel)
                except Reject:
                    return
                except:
                    exc_info = sys.exc_info()
                    ack(accept=False, silent=True)
                    self.raise_(reply_socket, channel, exc_info)
                    raise exc_info[0], exc_info[1], exc_info[2]
        else:
            ack(accept=True, silent=True)
            self.send_reply(reply_socket, RETURN, val, *channel)

    def find_call_target(self, call):
        try:
            return self.rpc_table[call.name]
        except KeyError:
            return getattr(self.app, call.name), default_rpc_spec

    def call(self, call, ack, f=None, rpc_spec=None):
        if f is None and rpc_spec is None:
            f, rpc_spec = self.find_call_target(call)
        args = call.args
        if rpc_spec.manual_ack:
            args = (ack,) + args
        return f(*args, **call.kwargs)

    def accept(self, reply_socket, channel):
        """Sends ACCEPT reply."""
        self.send_reply(reply_socket, ACCEPT, self.info, *channel)

    def reject(self, reply_socket, call, prefix=None):
        """Sends REJECT reply."""
        self.send_reply(reply_socket, REJECT, self.info,
                        call.call_id, None, prefix)

    def raise_(self, reply_socket, channel, exc_info=None):
        """Sends RAISE reply."""
        if not reply_socket:
            return
        if exc_info is None:
            exc_info = sys.exc_info()
        exc_type, exc, tb = exc_info
        while tb.tb_next is not None:
            tb = tb.tb_next
        filename, lineno = tb.tb_frame.f_code.co_filename, tb.tb_lineno
        val = (exc_type, str(exc), filename, lineno)
        try:
            state = exc.__getstate__()
        except AttributeError:
            pass
        else:
            val += (state,)
        self.send_reply(reply_socket, RAISE, val, *channel)

    @contextmanager
    def catch_exceptions(self):
        try:
            yield
        except:
            if self.exception_handler is not None:
                exc_info = sys.exc_info()
                self.exception_handler(self, exc_info)

    def get_replier(self, socket, prefix, reply_to):
        if reply_to is None:
            return None, None
        elif reply_to is Duplex:
            return socket, prefix
        else:
            return self.reply_socket, reply_to

    def send_reply(self, socket, method, data, call_id, task_id, prefix=None):
        if not socket:
            return
        # normal tuple is faster than namedtuple.
        reply = (method, data, call_id, task_id)
        try:
            eintr_retry_zmq(send, socket, reply, zmq.NOBLOCK,
                            prefix=prefix, pack=self.pack)
        except (zmq.Again, zmq.ZMQError):
            pass  # ignore.

    def close(self):
        super(Worker, self).close()
        if self.reply_socket is not None:
            self.reply_socket.close()
        for socket in self.worker_sockets:
            socket.close()

    def __repr__(self):
        return '<{0} info={1!r}>'.format(class_name(self), self.info)


class _Caller(object):

    available_socket_types = NotImplemented
    timeout = NotImplemented

    socket = None
    collector = None
    pack = None

    def __init__(self, socket, collector=None, pack=PACK):
        verify_socket_types(self.__class__.__name__,
                            self.available_socket_types, socket)
        self.socket = socket
        self.collector = collector
        self.pack = pack

    def _call_nowait(self, name, args, kwargs, topic=None):
        call = (name, args, kwargs, None, None)
        try:
            eintr_retry_zmq(send, self.socket, call,
                            zmq.NOBLOCK, topic, self.pack)
        except zmq.Again:
            pass  # ignore.

    def _call(self, name, args, kwargs,
              topic=None, limit=None, retry=False, max_retries=None):
        """Allocates a call id and emit."""
        col = self.collector
        if not col.is_running():
            col.start()
        call_id = uuid4_bytes()
        reply_to = (Duplex if self.socket is col.socket else col.topic)
        # Normal tuple is faster than namedtuple.
        call = (name, args, kwargs, call_id, reply_to)
        # Use short names.
        def send_call():
            try:
                eintr_retry_zmq(send, self.socket, call,
                                zmq.NOBLOCK, topic, self.pack)
            except zmq.Again:
                raise Undelivered('emission was not delivered')
        col.prepare(call_id)
        send_call()
        return col.establish(call_id, self.timeout, limit,
                             send_call if retry else None,
                             max_retries=max_retries)

    def close(self):
        self.socket.close()


class Customer(_Caller):
    """Customer sends RPC calls to the workers.  But it could not receive the
    result by itself.  It should work with :class:`Collector` to receive
    worker's results.
    """

    available_socket_types = [zmq.PAIR, zmq.DEALER, zmq.PUSH]
    timeout = CUSTOMER_TIMEOUT
    max_retries = None

    def call(self, *args, **kwargs):
        name, args = args[0], args[1:]
        if self.collector is None:
            return self._call_nowait(name, args, kwargs)
        results = self._call(name, args, kwargs, limit=1,
                             retry=True, max_retries=self.max_retries)
        return results[0]


class Fanout(_Caller):
    """Customer sends RPC calls to the workers.  But it could not receive the
    result by itself.  It should work with :class:`Collector` to receive
    worker's results.
    """

    available_socket_types = [zmq.PUB, ZMQ_XPUB]
    timeout = FANOUT_TIMEOUT

    def emit(self, *args, **kwargs):
        topic, name, args = args[0], args[1], args[2:]
        if self.collector is None:
            return self._call_nowait(name, args, kwargs, topic=topic)
        try:
            return self._call(name, args, kwargs, topic=topic)
        except EmissionError:
            return []


class Collector(Background):
    """Collector receives results from the worker."""

    socket = None
    topic = None
    unpack = None

    def __init__(self, socket, topic=None, unpack=UNPACK):
        super(Collector, self).__init__()
        verify_socket_types(self.__class__.__name__, [
            zmq.PAIR, zmq.DEALER, zmq.PULL, zmq.SUB, zmq.XSUB
        ], socket)
        self.socket = socket
        self.topic = topic
        self.unpack = unpack
        self.results = {}
        self.result_queues = {}

    def prepare(self, call_id):
        if call_id in self.results:
            raise KeyError('call-{0} already prepared'.format(call_id))
        self.results[call_id] = {}
        self.result_queues[call_id] = Queue()

    def establish(self, call_id, timeout, limit=None,
                  retry=None, max_retries=None):
        """Waits for the call is accepted by workers and starts to collect the
        results.
        """
        rejected = 0
        retried = 0
        results = []
        result_queue = self.result_queues[call_id]
        try:
            with Timeout(timeout, False):
                while True:
                    result = result_queue.get()
                    if result is None:
                        rejected += 1
                        if retry is not None:
                            if retried == max_retries:
                                break
                            retry()
                            retried += 1
                        continue
                    results.append(result)
                    if len(results) == limit:
                        break
        finally:
            del result_queue
            self.remove_result_queue(call_id)
        if not results:
            if rejected:
                raise Rejected('{0} workers rejected'.format(rejected)
                               if rejected != 1 else 'A worker rejected')
            else:
                raise WorkerNotFound('failed to find worker')
        return results

    def __call__(self):
        while True:
            try:
                __, args = recv(self.socket, unpack=self.unpack)
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
            reply = Reply(*args)
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
                raise KeyError('already established or unprepared call')
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

    def close(self):
        super(Collector, self).close()
        self.socket.close()
