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
from .helpers import class_name, eintr_retry_zmq as safe, Flag
from .messaging import (
    ACCEPT, ACK, BREAK, Call, PACK, RAISE, recv, REJECT, Reply, RETURN, send,
    UNPACK, YIELD)
from .results import RemoteResult


__all__ = ['Worker', 'Customer', 'Fanout', 'Collector']


# Compatible zmq constants:
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


# Default timeouts:
CUSTOMER_TIMEOUT = 5
FANOUT_TIMEOUT = 0.1


# Used for a value for `reply_to`:
NO_REPLY = '\x00'
DUPLEX = '\x01'


class Background(object):
    """A background object spawns only one greenlet at a time.  The greenlet
    will call its :meth:`__call__`.
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
        return
    __, __, topics = channel
    worker.reject(reply_socket, call.call_id, topics)
    if not silent:
        raise Reject


class Worker(Background):
    """A worker runs an RPC service of an object through ZeroMQ sockets:

    ::

       import os
       worker = Worker(os, [sock1, sock2], sock3, info='doctor')
       worker.run()

    :param app: an application to be shared by an RPC service.
    :param sockets: ZeroMQ sockets to receive RPC calls.
    :param reply_socket: a ZeroMQ socket to send RPC replies.
    :param info: (optional) a worker will send this value to callers at
                 accepting an call.  It might be the identity of the worker to
                 let the callers know which worker accepted.
    """

    sockets = None
    reply_socket = None
    info = None
    greenlet_group = None
    exception_handler = None
    malformed_message_handler = None
    pack = None
    unpack = None

    def __init__(self, app, sockets, reply_socket=None,
                 info=None, greenlet_group=None,
                 exception_handler=default_exception_handler,
                 malformed_message_handler=default_malformed_message_handler,
                 reject_if=(lambda *__: False), pack=PACK, unpack=UNPACK):
        super(Worker, self).__init__()
        self.app = app
        self.sockets = sockets
        self.reply_socket = reply_socket
        self.info = info
        if greenlet_group is None:
            greenlet_group = Group()
        self.greenlet_group = greenlet_group
        self.exception_handler = exception_handler
        self.malformed_message_handler = malformed_message_handler
        self.reject_if = reject_if
        self.pack = pack
        self.unpack = unpack

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
        for socket in self.sockets:
            poller.register(socket, zmq.POLLIN)
        group = self.greenlet_group
        msgs = []
        capture = msgs.extend
        def accept(socket, call, args, kwargs, topics):
            group.spawn(self.work, socket, call, args, kwargs, topics)
            group.join(0)
        def reject(socket, (__, call_id, reply_to), topics):
            reply_socket, topics = self.replier(socket, topics, reply_to)
            self.reject(reply_socket, call_id, topics)
        try:
            while True:
                for socket, event in safe(poller.poll):
                    assert event & zmq.POLLIN
                    del msgs[:]
                    try:
                        header, payload, topics = recv(socket, capture=capture)
                        call = Call(*header)
                        if group.full() or self.reject_if(topics, call):
                            # Reject the call if it should.
                            reject(socket, call, topics)
                            continue
                        args, kwargs = self.unpack(payload)
                    except:
                        # If any exception occurs in the above block,
                        # the messages are treated as malformed.
                        handle = self.malformed_message_handler
                        if handle is not None:
                            exc_info = sys.exc_info()
                            handle(self, exc_info, msgs[:])
                        del handle
                        continue
                    # Accept the call.
                    accept(socket, call, args, kwargs, topics)
                # Release memory.
                try:
                    del header, payload, topics
                    del call
                    del args, kwargs
                except UnboundLocalError:
                    # Stop at the first error.
                    pass
        finally:
            group.kill()

    def work(self, socket, call, args, kwargs, topics=()):
        """Calls a function and send results to the collector.  It supports
        all of function actions.  A function could return, yield, raise any
        packable objects.
        """
        task_id = uuid4_bytes()
        reply_socket, topics = self.replier(socket, topics, call.reply_to)
        if reply_socket:
            channel = (call.call_id, task_id, topics)
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
                val = self.call(call, args, kwargs, ack, f, rpc_spec)
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

    def call(self, call, args, kwargs, ack, f=None, rpc_spec=None):
        if f is None and rpc_spec is None:
            f, rpc_spec = self.find_call_target(call)
        if rpc_spec.manual_ack:
            args = (ack,) + args
        return f(*args, **kwargs)

    def accept(self, reply_socket, channel):
        """Sends ACCEPT reply."""
        self.send_reply(reply_socket, ACCEPT, self.info, *channel)

    def reject(self, reply_socket, call_id, topics=()):
        """Sends REJECT reply."""
        self.send_reply(reply_socket, REJECT, self.info,
                        call_id, '', topics)

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

    def replier(self, socket, topics, reply_to):
        if reply_to == NO_REPLY:
            return None, ()
        elif reply_to == DUPLEX:
            return socket, topics
        else:
            return self.reply_socket, (reply_to,)

    def send_reply(self, socket, method, value, call_id, task_id, topics=()):
        if not socket:
            return
        # normal tuple is faster than namedtuple.
        header = [chr(method), call_id, task_id]
        payload = self.pack(value)
        try:
            safe(send, socket, header, payload, topics, zmq.NOBLOCK)
        except (zmq.Again, zmq.ZMQError):
            pass  # ignore.

    def close(self):
        super(Worker, self).close()
        if self.reply_socket is not None:
            self.reply_socket.close()
        for socket in self.sockets:
            socket.close()

    def join(self, timeout=None, raise_error=False):
        return self.greenlet_group.join(timeout, raise_error)

    def __repr__(self):
        return '<{0} info={1!r}>'.format(class_name(self), self.info)


class _Caller(object):
    """A caller sends RPC calls to workers.  But it could not receive results
    from the workers by itself.  To receive the results, it should work with
    :class:`Collector` together.
    """

    timeout = NotImplemented

    socket = None
    collector = None
    pack = None

    def __init__(self, socket, collector=None, timeout=None, pack=PACK):
        self.socket = socket
        self.collector = collector
        self.pack = pack
        if timeout is not None:
            self.timeout = timeout

    def _call_nowait(self, name, args, kwargs, topics=()):
        header = [name, '', NO_REPLY]
        payload = self.pack((args, kwargs))
        try:
            safe(send, self.socket, header, payload, topics, zmq.NOBLOCK)
        except zmq.Again:
            pass  # ignore.

    def _call(self, name, args, kwargs, topics=(),
              limit=None, retry=False, max_retries=None):
        """Allocates a call id and emit."""
        col = self.collector
        if not col.is_running():
            col.start()
        call_id = uuid4_bytes()
        reply_to = (DUPLEX if self.socket is col.socket else col.topic)
        # Normal tuple is faster than namedtuple.
        header = [name, call_id, reply_to]
        payload = self.pack((args, kwargs))
        # Use short names.
        def send_call():
            try:
                safe(send, self.socket, header, payload, topics, zmq.NOBLOCK)
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
    """A customer is a caller that sends an RPC call to one of workers at once.
    """

    timeout = CUSTOMER_TIMEOUT
    max_retries = None

    def __init__(self, *args, **kwargs):
        max_retries = kwargs.pop('max_retries', None)
        if max_retries is not None:
            self.max_retries = max_retries
        super(Customer, self).__init__(*args, **kwargs)

    def call(self, *args, **kwargs):
        name, args = args[0], args[1:]
        if self.collector is None:
            self._call_nowait(name, args, kwargs)
            return
        results = self._call(name, args, kwargs, limit=1,
                             retry=True, max_retries=self.max_retries)
        return results[0]


class Fanout(_Caller):
    """A fanout is a caller that sends an RPC call to all workers subscribing
    the topic of the call at once.

    :param drop_if: (optional) a function which determines to drop RPC calls by
                    the topics it takes.  This parameter allows only as a
                    keyword-argument.

                    If you already know the list of topics which have one or
                    more subscribers, drop unnecessary RPC calls to reduce
                    serialization cost.  The XPUB socket type may can help you
                    to detect necessary topics.
    """

    timeout = FANOUT_TIMEOUT
    drop_if = None

    def __init__(self, *args, **kwargs):
        self.drop_if = kwargs.pop('drop_if', None)
        super(Fanout, self).__init__(*args, **kwargs)

    def emit(self, *args, **kwargs):
        topic = args[0]
        if self.drop_if is not None and self.drop_if(topic):
            # Drop the call without emission.
            return None if self.collector is None else []
        name, args = args[1], args[2:]
        topics = (topic,) if topic else ()
        if self.collector is None:
            self._call_nowait(name, args, kwargs, topics)
            return
        try:
            return self._call(name, args, kwargs, topics)
        except EmissionError:
            return []


class Collector(Background):
    """A collector receives RPC results from workers."""

    socket = None
    topic = None
    unpack = None

    def __init__(self, socket, topic='', unpack=UNPACK):
        super(Collector, self).__init__()
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
                header, payload, __ = recv(self.socket)
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
            method, call_id, reply_id = header
            reply = Reply(ord(method), call_id, reply_id)
            value = self.unpack(payload)
            del header, payload, method, call_id, reply_id
            try:
                self.dispatch_reply(reply, value)
            except KeyError:
                # TODO: warning
                continue
            finally:
                del reply, value

    def dispatch_reply(self, reply, value):
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
                worker_info = value
                result = RemoteResult(self, call_id, task_id, worker_info)
                self.results[call_id][task_id] = result
                result_queue.put_nowait(result)
            elif method == REJECT:
                result_queue.put_nowait(None)
        else:
            result = self.results[call_id][task_id]
            result.set_reply(reply.method, value)

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
