# -*- coding: utf-8 -*-
"""
    zeronimo.exceptions
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import zmq


__all__ = ['RemoteException', 'ZeronimoException', 'WorkerNotFound',
           'WorkerNotReachable', 'TaskRejected', 'SocketClosed',
           'MalformedMessage']


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
