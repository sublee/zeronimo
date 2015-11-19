# -*- coding: utf-8 -*-
"""
   zeronimo
   ~~~~~~~~

   RPC between distributed workers.

   :copyright: (c) 2013-2015 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .core import Collector, Customer, Fanout, Worker
from .exceptions import (
    EmissionError, MalformedMessage, Rejected, TaskClosed, TaskError,
    Undelivered, WorkerNotFound, ZeronimoException)
from .results import RemoteException, RemoteResult


__version__ = '0.2.9'
__all__ = [
    # components
    'Worker', 'Customer', 'Fanout', 'Collector',
    # exceptions
    'ZeronimoException', 'EmissionError', 'TaskError', 'WorkerNotFound',
    'Rejected', 'Undelivered', 'TaskClosed', 'MalformedMessage',
    # results
    'RemoteResult', 'RemoteException',
]
