# -*- coding: utf-8 -*-
"""
    zeronimo.exceptions
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from contextlib import contextmanager

import zmq


__all__ = ['ZeronimoException', 'WorkerNotFound', 'TaskRejected',
           'SocketClosed', 'UnexpectedMessage',
           'make_worker_not_found', 'raises']


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


class UnexpectedMessage(ZeronimoException, RuntimeWarning):
    """Warns when a received message is not expected format."""

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
    if rejected:
        exctype = TaskRejected
    else:
        exctype = WorkerNotFound
    if rejected == 1:
        errmsg.append('a worker rejected')
    elif rejected:
        errmsg.append('{0} workers rejected'.format(rejected))
    exc = exctype(', '.join(errmsg))
    return exc


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
