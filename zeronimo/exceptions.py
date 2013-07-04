# -*- coding: utf-8 -*-
"""
    zeronimo.exceptions
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""


__all__ = ['ZeronimoException', 'WorkerNotFound', 'make_worker_not_found']


class ZeronimoException(Exception):
    """The base exception for exceptions Zeronimo customized."""

    pass


class WorkerNotFound(ZeronimoException, LookupError):
    """Occurs by a collector which failed to find any worker accepted within
    the timeout.
    """

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
    if rejected == 1:
        errmsg.append('a worker rejected')
    elif rejected:
        errmsg.append('{0} workers rejected'.format(rejected))
    err = WorkerNotFound(', '.join(errmsg))
    err.rejected = rejected
    return err
