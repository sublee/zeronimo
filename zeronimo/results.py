# -*- coding: utf-8 -*-
"""
   zeronimo.results
   ~~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from binascii import hexlify

from gevent.event import AsyncResult
from gevent.queue import Queue

from zeronimo.exceptions import TaskClosed
from zeronimo.helpers import make_repr
from zeronimo.messaging import BREAK, DONE, RAISE, RETURN, YIELD


__all__ = ['RemoteResult', 'RemoteException', 'RemoteIterator']


class RemoteResult(AsyncResult):
    """The task object.

    :param customer: the customer object.
    :param id: the task identifier.
    :param invoker_id: the identifier of the invoker which spawned this task.
    :param worker_info: the value the worker sent at accepting.
    """

    def __init__(self, collector, call_id, task_id, worker_info=None):
        super(RemoteResult, self).__init__()
        self.collector = collector
        self.call_id = call_id
        self.task_id = task_id
        self.worker_info = worker_info

    def close(self):
        """Stops to collect replies from its task."""
        self.set_exception(TaskClosed)
        self.collector.remove_result(self)

    # iterator

    _iterator = False

    def is_iterator(self):
        return self._iterator

    def set_iterator(self):
        self._iterator = True
        self.set(RemoteIterator())

    # exception

    def set_remote_exception(self, remote_exc_info):
        """Raises an exception as a :exc:`RemoteException`."""
        exc_type, exc_str, filename, lineno = remote_exc_info[:4]
        exc_type = RemoteException.compose(exc_type)
        exc = exc_type(exc_str, filename, lineno, self.worker_info)
        if len(remote_exc_info) > 4:
            state = remote_exc_info[4]
            exc.__setstate__(state)
        self.set_exception(exc)

    def set_exception(self, exc):
        if self.is_iterator():
            self.get().throw(exc)
        else:
            super(RemoteResult, self).set_exception(exc)

    # reply receivers

    def set_reply(self, method, value):
        if method == RETURN:
            self._return(value)
        elif method == YIELD:
            self._yield(value)
        elif method == RAISE:
            self._raise(value)
        elif method == BREAK:
            self._break(value)
        if method & DONE:
            self.collector.remove_result(self)

    def _return(self, value):
        self.set(value)

    def _yield(self, value):
        if not self.is_iterator():
            self.set_iterator()
        self.get().send(value)

    def _raise(self, value):
        self.set_remote_exception(value)

    def _break(self, value):
        if self.is_iterator():
            self.get().close()
        else:
            self.set(iter([]))

    def __repr__(self):
        return make_repr(self, None, ['call_id', 'task_id', 'worker_info'],
                         reprs={'call_id': hexlify, 'task_id': hexlify})


class RemoteException(BaseException):

    _composed = {}

    @classmethod
    def compose(cls, exc_type):
        try:
            return cls._composed[exc_type]
        except KeyError:
            class composed_exc_type(exc_type, cls):
                __init__ = cls.__init__
            composed_exc_type.exc_type = exc_type
            composed_exc_type.exctype = exc_type  # For backward compatibility.
            composed_exc_type.__name__ = exc_type.__name__ + '(Remote)'
            # Avoid to start with dot in traceback.
            composed_exc_type.__module__ = 'exceptions'
            cls._composed[exc_type] = composed_exc_type
            return composed_exc_type

    def __init__(self, message, filename=None, lineno=None, worker_info=None):
        super(RemoteException, self).__init__(message)
        self.filename = filename
        self.lineno = lineno
        self.worker_info = worker_info

    def __str__(self):
        string = super(RemoteException, self).__str__()
        if self.filename is not None:
            string += ' ({0}:{1})'.format(self.filename, self.lineno)
        return string


class RemoteIterator(object):

    def __init__(self):
        self.queue = Queue()

    def __iter__(self):
        return self

    def send(self, value):
        if self.queue is None:
            raise StopIteration
        self.queue.put((True, value))

    def throw(self, exc):
        if self.queue is None:
            raise StopIteration
        self.queue.put((False, exc))

    def close(self):
        self.throw(StopIteration)

    def next(self):
        if self.queue is None:
            raise StopIteration
        yields, value = self.queue.get()
        if yields:
            return value
        else:
            self.queue = None
            raise value
