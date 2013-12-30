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
