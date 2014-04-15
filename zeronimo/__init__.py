# -*- coding: utf-8 -*-
"""
    zeronimo
    ~~~~~~~~

    RPC to distributed workers.

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import

from .core import Worker, Customer, Fanout, Collector
from .exceptions import (
    ZeronimoException, EmissionError, TaskError, WorkerNotFound, Rejected,
    Undelivered, TaskClosed, MalformedMessage)
from .results import RemoteResult, RemoteException


__version__ = '0.2.2'
__all__ = [
    # components
    'Worker', 'Customer', 'Fanout', 'Collector',
    # exceptions
    'ZeronimoException', 'EmissionError', 'TaskError', 'WorkerNotFound',
    'Rejected', 'Undelivered', 'TaskClosed', 'MalformedMessage',
    # results
    'RemoteResult', 'RemoteException']
