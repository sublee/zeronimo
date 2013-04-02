# -*- coding: utf-8 -*-
"""
    zeronimo
    ~~~~~~~~

    A distributed RPC solution based on ØMQ.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import

from .collect import register, collect_remote_functions
from .core import Worker, Customer, Tunnel, Task


__version__ = '0.0.dev'
__all__ = ['register', 'collect_remote_functions',
           'Worker', 'Customer', 'Tunnel', 'Task']
