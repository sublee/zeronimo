# -*- coding: utf-8 -*-
"""
   zeronimo
   ~~~~~~~~

   RPC between distributed workers.

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from zeronimo.__about__ import __version__  # noqa
from zeronimo.application import get_rpc_spec, rpc
from zeronimo.core import Collector, Customer, Fanout, Worker
from zeronimo.exceptions import (
    EmissionError, MalformedMessage, Rejected, TaskClosed, TaskError,
    Undelivered, WorkerNotFound, ZeronimoException)
from zeronimo.results import RemoteException, RemoteResult


__all__ = [
    # application
    'rpc', 'get_rpc_spec',
    # components
    'Worker', 'Customer', 'Fanout', 'Collector',
    # exceptions
    'ZeronimoException', 'EmissionError', 'TaskError', 'WorkerNotFound',
    'Rejected', 'Undelivered', 'TaskClosed', 'MalformedMessage',
    # results
    'RemoteResult', 'RemoteException',
]
