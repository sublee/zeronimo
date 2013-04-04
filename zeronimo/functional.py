# -*- coding: utf-8 -*-
"""
    zeronimo.functional
    ~~~~~~~~~~~~~~~~~~~

    Provides higher-order functions.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import Iterable, Sequence, Set, Mapping, namedtuple
import functools
import hashlib

from gevent.coros import Semaphore


def remote(func):
    """This decorator makes a function to be collected by
    :func:`collect_remote_functions` for being invokable by remote clients.
    """
    func._znm = True
    return func


def collect_remote_functions(obj):
    """Collects remote functions from the object."""
    functions = {}
    for attr in dir(obj):
        func = getattr(obj, attr)
        if hasattr(func, '_znm') and func._znm:
            functions[attr] = func
    return functions


def should_yield(val):
    return (
        isinstance(val, Iterable) and
        not isinstance(val, (Sequence, Set, Mapping)))
