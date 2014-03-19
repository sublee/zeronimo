# -*- coding: utf-8 -*-
"""
    zeronimo.exceptions
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from contextlib import contextmanager

import zmq


__all__ = ['ZeronimoException', 'WorkerNotFound', 'WorkerNotReachable',
           'TaskRejected', 'SocketClosed', 'MalformedMessage', 'raises']


class ZeronimoException(BaseException):
    """The base exception for exceptions Zeronimo customized."""

    pass


class WorkerNotFound(ZeronimoException):
    """Occurs by a collector which failed to find any worker accepted within
    the timeout.
    """

    pass


class WorkerNotReachable(WorkerNotFound):
    """The customer has no connection to any worker."""

    pass


class TaskRejected(WorkerNotFound):
    """Failed to find worker accepts the task within the timeout."""

    pass


class SocketClosed(ZeronimoException, zmq.ZMQError):
    """Collector socket is closed while a task is collecting replies."""

    pass


class MalformedMessage(ZeronimoException, RuntimeWarning):
    """Warns when a received message is not expected format."""

    pass


@contextmanager
def raises(exctype):
    """Expected exceptions occured in the context will not raised at the
    worker's greenlet. The exceptions will still be delivered to the collector.

    Use this in methods of the application object.
    """
    try:
        yield
    except exctype as exc:
        exc._znm_expected = True
        raise


class RemoteException(BaseException):

    _composed = {}

    @classmethod
    def compose(cls, exctype):
        try:
            return cls._composed[exctype]
        except KeyError:
            class composed_exctype(exctype, cls):
                __init__ = cls.__init__
            composed_exctype.exctype = exctype
            composed_exctype.__name__ = exctype.__name__ + '(Remote)'
            # avoid to start with dot in traceback
            composed_exctype.__module__ = 'exceptions'
            cls._composed[exctype] = composed_exctype
            return composed_exctype

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
