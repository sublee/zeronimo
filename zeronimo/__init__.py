# -*- coding: utf-8 -*-
"""
   zeronimo
   ~~~~~~~~

   RPC between distributed workers.

   :copyright: (c) 2013-2015 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .__about__ import __version__  # noqa
from .application import get_rpc_mark, rpc
from .core import Collector, Customer, Fanout, Worker
from .exceptions import (
    EmissionError, MalformedMessage, Reject, Rejected, TaskClosed, TaskError,
    Undelivered, WorkerNotFound, ZeronimoException)
from .results import RemoteException, RemoteResult


__all__ = [
    # application
    'rpc', 'get_rpc_mark',
    # components
    'Worker', 'Customer', 'Fanout', 'Collector',
    # exceptions
    'ZeronimoException', 'Reject', 'EmissionError', 'TaskError',
    'WorkerNotFound', 'Rejected', 'Undelivered', 'TaskClosed',
    'MalformedMessage',
    # results
    'RemoteResult', 'RemoteException',
]
