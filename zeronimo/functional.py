# -*- coding: utf-8 -*-
"""
    zeronimo.functional
    ~~~~~~~~~~~~~~~~~~~

    Provides higher-order functions.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import Iterable, Sequence, Set, Mapping, namedtuple


Plan = namedtuple('Plan', ['fanout', 'reply'])


def register(f=None, fanout=False, reply=True):
    """This decorator makes a function to be collected by
    :func:`collect_remote_functions` for being invokable by remote clients.
    """
    plan = Plan(fanout, reply)
    def decorator(f):
        f._znm_plan = plan
        return f
    return decorator(f) if f is not None else decorator


def collect_remote_functions(obj):
    """Collects remote functions from the object."""
    for attr in dir(obj):
        func = getattr(obj, attr)
        try:
            plan = func._znm_plan
        except AttributeError:
            continue
        yield func, plan


def should_yield(val):
    return (isinstance(val, Iterable) and
            not isinstance(val, (Sequence, Set, Mapping)))
