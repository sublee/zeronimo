# -*- coding: utf-8 -*-
"""
    zeronimo
    ~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import namedtuple, Iterable, Sequence, Set, Mapping
import functools
import msgpack
import pickle
import re
from types import MethodType
import uuid

from gevent import spawn, Timeout
from gevent.coros import Semaphore
from gevent.event import Event, AsyncResult
from gevent.queue import Queue, Empty
import zmq.green as zmq


__version__ = '0.0.dev'
__all__ = ['Worker', 'Customer', 'Tunnel', 'Invoker', 'Task']


# utility functions


def alloc_id(exclusive=None):
    id = None
    while id is None or exclusive is not None and id in exclusive:
        id = str(uuid.uuid4())[:6]
    return id


def poll_or_stopped(poller, stopper):
    waiting_stop = spawn(stopper.wait)
    try:
        waiting_stop.get(block=False)
    except Timeout:
        pass
    else:
        return True
    async_result = AsyncResult()
    waiting_stop.link(async_result)
    polling = spawn(poller.poll)
    polling.link(async_result)
    try:
        return async_result.get()
    finally:
        polling.kill()
        waiting_stop.kill()


def should_yield(val):
    serializable = (Sequence, Set, Mapping)
    return (isinstance(val, Iterable) and not isinstance(val, serializable))


def read_socket_type(socket_type):
    return {
        zmq.PAIR: 'PAIR', zmq.PUB: 'PUB', zmq.SUB: 'SUB', zmq.REQ: 'REQ',
        zmq.REP: 'REP', zmq.DEALER: 'DEALER', zmq.ROUTER: 'ROUTER',
        zmq.PULL: 'PULL', zmq.PUSH: 'PUSH', zmq.XPUB: 'XPUB', zmq.XSUB: 'XSUB'
    }[socket_type]


def get_socket(sockets, socket_type, name=None):
    try:
        return sockets[socket_type]
    except KeyError:
        msg = 'no {0} socket'.format(read_socket_type(socket_type))
        if name is None:
            msg = 'There\'s ' + msg
        else:
            msg = '{0} has {1}'.format(name, msg)
        raise KeyError(msg)


def make_repr(obj, params=[], keywords=[], data={}):
    get = lambda attr: data[attr] if attr in data else getattr(obj, attr)
    opts = []
    if params:
        opts.append(', '.join([repr(get(attr)) for attr in params]))
    if keywords:
        opts.append(', '.join(
            ['{0}={1!r}'.format(attr, get(attr)) for attr in keywords]))
    return '{0}({1})'.format(cls_name(obj), ', '.join(opts))


def cls_name(obj):
    return type(obj).__name__


# wrapped ZMQ functions


prefix_sep = chr(0)
prefix_pattern = re.compile(r'^[^{0}]*{0}'.format(prefix_sep))


def default(obj):
    return {'pickle': pickle.dumps(obj)}


def object_hook(obj):
    if 'pickle' in obj:
        return pickle.loads(obj['pickle'])
    return obj


def send(sock, obj, flags=0, prefix=''):
    """Same with :meth:`zmq.Socket.send_pyobj` but can append prefix for
    filtering subscription.
    """
    assert prefix_sep not in prefix
    serial = msgpack.packb(obj, default=default)
    msg = prefix_sep.join([prefix, serial])
    return sock.send(msg, flags)


def recv(sock, flags=0):
    """Same with :meth:`zmq.Socket.recv_pyobj`."""
    msg = sock.recv(flags)
    serial = prefix_pattern.sub('', msg)
    return msgpack.unpackb(serial, object_hook=object_hook)


# exceptions


class ZeronimoException(Exception):

    pass


class WorkerNotEnough(ZeronimoException, LookupError):

    pass


class WorkerNotFound(WorkerNotEnough):

    pass


# message frames


_Invocation = namedtuple('Invocation', ['function_name', 'args', 'kwargs',
                                        'invoker_id', 'customer_addr'])
_Reply = namedtuple('Reply', ['method', 'data', 'invoker_id', 'task_id'])


class Invocation(_Invocation):

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


# reply methods


ACCEPT = 1
REJECT = 0
RETURN = 100
RAISE = 101
YIELD = 102
BREAK = 103


# models


class Runner(object):
    """A runner should implement :meth:`run`. :attr:`running` is ensured to be
    ``True`` while :meth:`run` is runnig.
    """

    def __new__(cls, *args, **kwargs):
        obj = super(Runner, cls).__new__(cls)
        stopper = Event()
        cls._patch_run(obj, stopper)
        cls._patch_stop(obj, stopper)
        return obj

    @classmethod
    def _patch_run(cls, obj, stopper):
        obj._running_lock = Semaphore()
        def run(self, starter=None):
            if self.is_running():
                if starter is not None:
                    starter.set()
                    return
                raise RuntimeError(
                    '{0} already running'.format(cls_name(self)))
            try:
                with self._running_lock:
                    starter and starter.set()
                    rv = obj._run(stopper)
            finally:
                try:
                    del self._async_running
                except AttributeError:
                    pass
                stopper.clear()
            return rv
        obj.run, obj._run = MethodType(run, obj), obj.run

    @classmethod
    def _patch_stop(cls, obj, stopper):
        def stop(self):
            if not self.is_running():
                raise RuntimeError('{0} not running'.format(cls_name(self)))
            stopper.set()
            return obj._stop()
        obj.stop, obj._stop = MethodType(stop, obj), obj.stop

    def run(self, stopper):
        raise NotImplementedError(
            '{0} has not implementation to run'.format(cls_name(self)))

    def stop(self):
        self.wait()

    def is_running(self):
        return self._running_lock.locked()

    def start(self):
        if self.is_running():
            raise RuntimeError('{0} already running'.format(cls_name(self)))
        starter = Event()
        self._async_running = spawn(self.run, starter=starter)
        starter.wait()

    def join(self, block=True, timeout=None):
        try:
            return self._async_running.get(block, timeout)
        except AttributeError:
            raise RuntimeError(
                '{0} running in foreground'.format(cls_name(self)))

    def wait(self, timeout=None):
        self._running_lock.wait(timeout)


class Worker(Runner):
    """The worker object runs an RPC service of an object through ZMQ sockets.
    The ZMQ sockets should be PULL or SUB socket type. The PULL sockets receive
    Round-robin invocations; the SUB sockets receive Publish-subscribe
    (fan-out) invocations.

    ::

       import os
       worker = Worker(os, [sock1, sock2], info='doctor')
       worker.run()

    :param obj: the object to be shared by an RPC service.
    :param sockets: the ZMQ sockets of PULL or SUB socket type.
    :param info: (optional) the worker will send this value to customers at
                 accepting an invocation. it might be identity of the worker to
                 let the customer's know what worker accepted.
    """

    obj = None
    sockets = None
    info = None

    def __init__(self, obj, sockets, info=None):
        super(Worker, self).__init__()
        self.obj = obj
        socket_types = set(sock.socket_type for sock in sockets)
        if socket_types.difference([zmq.PULL, zmq.SUB]):
            raise ValueError('Worker socket should be PULL or SUB')
        self.sockets = sockets
        self.info = info
        self.accept_all()

    def accept_all(self):
        """After calling this, the worker will accept all invocations. This
        will be called at the initialization of the worker.
        """
        self.accepting = True

    def reject_all(self):
        """After calling this, the worker will reject all invocations. If the
        worker is busy, it will be helpful.
        """
        self.accepting = False

    def run(self, stopper):
        """Runs the worker. While running, an RPC service is online."""
        poller = zmq.Poller()
        for sock in self.sockets:
            poller.register(sock, zmq.POLLIN)
        while not stopper.is_set():
            events = poll_or_stopped(poller, stopper)
            if events is True:  # has been stopped
                break
            for sock, event in events:
                if event & zmq.POLLIN:
                    invocation = Invocation(*recv(sock))
                    spawn(self.run_task, invocation, sock.context)
                if event & zmq.POLLERR:
                    assert 0

    def run_task(self, invocation, context):
        """Invokes a function and send results to the customer. It supports
        all of function actions. A function could return, yield, raise any
        picklable objects.
        """
        task_id = alloc_id()
        function_name = invocation.function_name
        args = invocation.args
        kwargs = invocation.kwargs
        sock = False
        try:
            if invocation.customer_addr is not None:
                sock = context.socket(zmq.PUSH)
                sock.connect(invocation.customer_addr)
                channel = (invocation.invoker_id, task_id)
                method = ACCEPT if self.accepting else REJECT
                sock and self.send_reply(sock, method, self.info, *channel)
            if not self.accepting:
                return
            try:
                val = getattr(self.obj, function_name)(*args, **kwargs)
            except Exception as error:
                sock and self.send_reply(sock, RAISE, error, *channel)
                raise
            if should_yield(val):
                try:
                    for item in val:
                        sock and self.send_reply(sock, YIELD, item, *channel)
                except Exception as error:
                    sock and self.send_reply(sock, RAISE, error, *channel)
                else:
                    sock and self.send_reply(sock, BREAK, None, *channel)
            else:
                sock and self.send_reply(sock, RETURN, val, *channel)
        finally:
            sock and sock.close()

    def send_reply(self, sock, method, data, task_id, run_id):
        reply = Reply(method, data, task_id, run_id)
        return send(sock, tuple(reply))

    def __repr__(self):
        keywords = ['info'] if self.info is not None else []
        return make_repr(self, ['obj', 'sockets'], keywords)


class Customer(Runner):
    """The customer object makes a tunnel which links to workers and collects
    workers' replies.

    A customer has a PULL type socket to collect worker's replies and its
    public address what workers can connect.

    ::

       customer = Customer(socket_which_receive_replies, public_address)
       with customer.link([socket_which_connects_to_workers]) as tunnel:
           print tunnel.hello()

    :param socket: the ZMQ socket of PULL socket type.
    :param addr: the public address of the socket what workers can connect.
    """

    socket = None
    addr = None
    tunnels = None
    tasks = None

    def __init__(self, socket, addr):
        super(Customer, self).__init__()
        if socket.socket_type != zmq.PULL:
            raise ValueError('Customer socket should be PULL')
        self.socket = socket
        self.addr = addr
        self.tunnels = set()
        self.tasks = {}
        self.invokers = {}
        self._missings = {}

    def link(self, *args, **kwargs):
        """Creates a tunnel which uses the customer as a linked customer."""
        return Tunnel(self, *args, **kwargs)

    def register_tunnel(self, tunnel):
        """Registers a :class:`Tunnel` object.

        :returns: the tunnel registry.
        """
        self.tunnels.add(tunnel)
        return self.tunnels

    def unregister_tunnel(self, tunnel):
        """Unregisters a :class:`Tunnel` object.

        :returns: the tunnel registry.
        """
        self.tunnels.remove(tunnel)
        return self.tunnels

    def register_invoker(self, invoker):
        """Registers a :class:`Invoker` object.

        :returns: the invoker registry.
        """
        self.invokers[invoker.id] = invoker
        return self.invokers

    def unregister_invoker(self, invoker):
        """Unregisters a :class:`Invoker` object. It puts :exc:`StopIteration`
        to the invoker queue.

        :returns: the invoker registry.
        """
        assert self.invokers.pop(invoker.id) is invoker
        invoker.queue.put(StopIteration)
        return self.invokers

    def register_task(self, task):
        """Registers a :class:`Task` object. If there're missing messages for
        the task, it restores them.

        :returns: the task registry related to the same invoker.
        """
        try:
            self.tasks[task.invoker_id][task.id] = task
        except KeyError:
            self.tasks[task.invoker_id] = {task.id: task}
        self._restore_missing_messages(task)
        return self.tasks[task.invoker_id]

    def unregister_task(self, task):
        """Unregisters a :class:`Task` object.

        :returns: the task registry related to the same invoker.
        """
        tasks = self.tasks[task.invoker_id]
        assert tasks.pop(task.id) is task
        if not tasks:
            del self.tasks[task.invoker_id]
        return tasks

    def _restore_missing_messages(self, task):
        """Restores kept missing messages for the task."""
        try:
            missing = self._missings[task.invoker_id].pop(task.id)
        except KeyError:
            return
        if not self._missings[task.invoker_id]:
            del self._missings[task.invoker_id]
        try:
            while missing.queue:
                task.queue.put(missing.get(block=False))
        except Empty:
            pass

    def run(self, stopper):
        """Runs the customer. While running, it receives replies from the
        socket and dispatch them to put to the proper queue.
        """
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        while not stopper.is_set():
            events = poll_or_stopped(poller, stopper)
            if events is True:  # has been stopped
                break
            elif not events:  # polling timed out
                continue
            event = events[0][1]
            if event & zmq.POLLIN:
                reply = Reply(*recv(self.socket))
                self.dispatch_reply(reply)
            if event & zmq.POLLERR:
                assert 0

    def dispatch_reply(self, reply):
        """Puts a reply to the proper queue."""
        invoker_id = reply.invoker_id
        task_id = reply.task_id
        if reply.method in (ACCEPT, REJECT):
            try:
                queue = self.invokers[invoker_id].queue
            except KeyError:
                # drop message
                return
            # prepare collections for task messages
            if invoker_id not in self.tasks:
                self.tasks[invoker_id] = {}
            try:
                self._missings[invoker_id][task_id] = Queue()
            except KeyError:
                self._missings[invoker_id] = {task_id: Queue()}
        else:
            try:
                tasks = self.tasks[invoker_id]
            except KeyError:
                # drop message
                return
            try:
                queue = tasks[task_id].queue
            except KeyError:
                queue = self._missings[invoker_id][task_id]
        queue.put(reply)

    def __repr__(self):
        return make_repr(self, ['socket', 'addr'])


class Tunnel(object):
    """A session between the customer and the distributed workers. It can send
    a request of RPC through sockets on the customer's context.

    :param customer: the :class:`Customer` object or ``None``.
    :param sockets: the sockets which connect to the workers. it should be
                    one or none PUSH socket and one or none PUB socket.
    :param prefix: the topic the workers are subscribing.

    :param wait: (keyword-only) if it's set to ``True``, the workers will
                 reply. Otherwise, the workers just invoke a function without
                 reply. Defaults to ``True``.
    :param fanout: (keyword-only) if it's set to ``True``, all workers will
                   receive an invocation request. Defaults to ``False``.
    :param as_task: (keyword-only) actually, every remote function calls have
                    own :class:`Task` object. if it's set to ``True``, remote
                    functions return a :class:`Task` object instead of result
                    value. Defaults to ``False``.
    :param finding_timeout: (keyword-only) the seconds to timeout for
                            collecting workers which accepted the task.
                            Defaults to 0.01 seconds.
    """

    def __init__(self, customer, sockets, prefix='', **invoker_opts):
        self._znm_customer = customer
        self._znm_sockets = {}
        for sock in sockets:
            if (sock.socket_type not in (zmq.PUSH, zmq.PUB) or
                sock.socket_type in self._znm_sockets):
                raise ValueError(
                    'Tunnel allows only one or none PUSH socket and one or '
                    'none PUB socket')
            self._znm_sockets[sock.socket_type] = sock
        self._znm_prefix = prefix
        self._znm_invoker_opts = invoker_opts

    def __getattr__(self, attr):
        return functools.partial(self._znm_invoke, attr)

    def _znm_invoke(self, function_name, *args, **kwargs):
        """Invokes a remote function."""
        if self._znm_customer is None:
            invoker_id = None
        else:
            if not self._znm_customer.is_running():
                raise RuntimeError('Customer not running')
            invoker_id = alloc_id(self._znm_customer.invokers)
        invoker = Invoker(self, function_name, args, kwargs, invoker_id)
        return invoker.invoke(**self._znm_invoker_opts)

    def __enter__(self):
        customer = self._znm_customer
        if customer is not None:
            if customer.register_tunnel(self) and not customer.is_running():
                customer.start()
        return self

    def __exit__(self, error, error_type, traceback):
        customer = self._znm_customer
        if customer is not None:
            if not customer.unregister_tunnel(self):
                customer.stop()

    def __call__(self, **replacing_invoker_opts):
        """Creates a :class:`Tunnel` object which follows same consumer and
        workers but replaced invoker options.
        """
        invoker_opts = {}
        invoker_opts.update(self._znm_invoker_opts)
        invoker_opts.update(replacing_invoker_opts)
        customer, prefix = self._znm_customer, self._znm_prefix
        tunnel = Tunnel(customer, [], prefix, **invoker_opts)
        tunnel._znm_sockets = self._znm_sockets
        return tunnel

    def __repr__(self):
        params = ['customer', 'sockets']
        keywords = ['prefix'] + self._znm_invoker_opts.keys()
        data = {attr: getattr(self, '_znm_' + attr) for attr in params}
        data.update(self._znm_invoker_opts)
        return make_repr(self, params, keywords, data)


class Invoker(object):
    """The invoker object sends an invocation message to the workers which the
    tunnel connected.

    :param tunnel: the tunnel object.
    :param function_name: the function name.
    :param args: the tuple of the arguments.
    :param kwargs: the dictionary of the keyword arguments.
    :param id: the identifier.
    """

    def __init__(self, tunnel, function_name, args, kwargs, id):
        self.function_name = function_name
        self.args = args
        self.kwargs = kwargs
        self.tunnel = tunnel
        self.id = id
        self.queue = Queue()

    def __getattr__(self, attr):
        return getattr(self.tunnel, '_znm_' + attr)

    def invoke(self, wait=True, fanout=False, fanout_min=1, as_task=False,
               finding_timeout=0.01):
        if not wait:
            return self._invoke_nowait(fanout)
        if self.customer is None:
            raise ValueError(
                'To wait for a result, the tunnel must have a customer')
        if fanout:
            return self._invoke_fanout(as_task, finding_timeout, fanout_min)
        else:
            return self._invoke(as_task, finding_timeout)

    def _invoke_nowait(self, fanout):
        socket_type = zmq.PUB if fanout else zmq.PUSH
        sock = get_socket(self.sockets, socket_type, 'Tunnel')
        invocation = Invocation(
            self.function_name, self.args, self.kwargs, self.id, None)
        send(sock, tuple(invocation), prefix=self.prefix)

    def _invoke(self, as_task, finding_timeout):
        sock = get_socket(self.sockets, zmq.PUSH, 'Tunnel')
        invocation = Invocation(self.function_name, self.args, self.kwargs,
                                self.id, self.customer.addr)
        # find one worker
        self.customer.register_invoker(self)
        reply = None
        rejected = 0
        send(sock, tuple(invocation), prefix=self.prefix)
        try:
            with Timeout(finding_timeout, False):
                while True:
                    reply = self.queue.get()
                    if reply.method == REJECT:
                        rejected += 1
                        # send again
                        send(sock, tuple(invocation), prefix=self.prefix)
                        reply = None
                        continue
                    elif reply.method == ACCEPT:
                        break
                    else:
                        assert 0
        finally:
            self.customer.unregister_invoker(self)
        if reply is None:
            errmsg = 'Worker not found'
            if rejected == 1:
                errmsg += ', a worker rejected'
            elif rejected:
                errmsg += ', {0} workers rejected'.format(rejected)
            raise WorkerNotFound(errmsg)
        else:
            return self._spawn_task(reply, as_task)

    def _spawn_task(self, reply, as_task=False):
        assert reply.method == ACCEPT
        worker_info = reply.data and tuple(reply.data)
        task = Task(self.customer, reply.task_id, self.id, worker_info)
        return task if as_task else task()

    def _invoke_fanout(self, as_task, finding_timeout, fanout_min):
        sock = get_socket(self.sockets, zmq.PUB, 'Tunnel')
        invocation = Invocation(self.function_name, self.args, self.kwargs,
                                self.id, self.customer.addr)
        # find one or more workers
        self.customer.register_invoker(self)
        replies = []
        rejected = 0
        send(sock, tuple(invocation), prefix=self.prefix)
        with Timeout(finding_timeout, False):
            while True:
                reply = self.queue.get()
                if reply.method == REJECT:
                    rejected += 1
                elif reply.method == ACCEPT:
                    replies.append(reply)
                else:
                    assert 0
        num_replies = len(replies)
        if num_replies < fanout_min:
            if rejected == 1:
                after_errmsg = ', a worker rejected'
            elif rejected:
                after_errmsg = ', {0} workers rejected'.format(rejected)
            else:
                after_errmsg = ''
            if num_replies:
                if num_replies == 1:
                    errfmt = 'A worker replied but not enough'
                else:
                    errfmt = '{0} workers replied but not enough'
                errmsg = errfmt.format(num_replies)
                errcls = WorkerNotEnough
            else:
                errmsg = 'Worker not found'
                errcls = WorkerNotFound
            raise errcls(''.join([errmsg, after_errmsg]))
        return self._spawn_fanout_tasks(replies, as_task)

    def _spawn_fanout_tasks(self, replies, as_task=False):
        tasks = []
        iter_replies = iter(replies)
        def collect_tasks(getter):
            for reply in iter(getter, None):
                if reply is StopIteration:
                    break
                assert reply.method == ACCEPT
                worker_info = reply.data and tuple(reply.data)
                task = Task(self.customer, reply.task_id, self.id, worker_info)
                tasks.append(task if as_task else task())
        collect_tasks(iter_replies.next)
        spawn(collect_tasks, self.queue.get)
        return tasks

    def __repr__(self):
        return make_repr(self, ['function_name', 'args', 'kwargs'])


class Task(object):
    """The task object.

    :param customer: the customer object.
    :param id: the task identifier.
    :param invoker_id: the identifier of the invoker which spawned this task.
    :param worker_info: the value the worker sent at accepting.
    """

    def __init__(self, customer, id, invoker_id, worker_info=None):
        self.customer = customer
        self.id = id
        self.invoker_id = invoker_id
        self.worker_info = worker_info
        self.queue = Queue()

    def __call__(self):
        """Gets the result."""
        self.customer.register_task(self)
        reply = self.queue.get()
        assert reply.method not in (ACCEPT, REJECT)
        if reply.method in (RETURN, RAISE):
            if not self.customer.unregister_task(self):
                try:
                    invoker = self.customer.invokers[self.invoker_id]
                except KeyError:
                    pass
                else:
                    self.customer.unregister_invoker(invoker)
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
            assert reply.method not in (ACCEPT, REJECT, RETURN)
            if reply.method == YIELD:
                yield reply.data
            elif reply.method == RAISE:
                raise reply.data
            elif reply.method == BREAK:
                break

    def __repr__(self):
        return make_repr(
            self, ['customer', 'id'], ['invoker_id', 'worker_info'])
