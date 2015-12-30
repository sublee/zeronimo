# -*- coding: utf-8 -*-
"""
   zeronimo.application
   ~~~~~~~~~~~~~~~~~~~~

   A tools for application which a worker wraps.

   :copyright: (c) 2013-2015 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from collections import namedtuple
import functools
import inspect


__all__ = ['default_rpc_mark', 'rpc', 'rpc_table', 'get_rpc_mark']


RPC_MARK_ATTR = '__zeronimo_rpc__'


RPCMark = namedtuple('RPCMark', ['name', 'defer_ack', 'reject_on'])


def _mark_as_rpc(f, name=None, defer_ack=False, reject_on=None):
    if name is None:
        name = f.__name__
    if reject_on is not None and not defer_ack:
        raise ValueError('Set defer_ack=True to use reject_on option')
    rpc_mark = RPCMark(name, defer_ack, reject_on)
    setattr(f, RPC_MARK_ATTR, rpc_mark)
    return f


#: The :class:`RPCMark` with default values.
default_rpc_mark = RPCMark(*inspect.getargspec(_mark_as_rpc).defaults)


def rpc(f=None, **kwargs):
    """Mark a method as RPC."""
    if f is not None:
        if isinstance(f, basestring):
            if 'name' in kwargs:
                raise ValueError('name option duplicated')
            kwargs['name'] = f
        else:
            return rpc(**kwargs)(f)
    return functools.partial(_mark_as_rpc, **kwargs)


def rpc_table(app):
    """Collects methods which are marked as RPC."""
    table = {}
    for attr, value in inspect.getmembers(app):
        rpc_mark = get_rpc_mark(value, default=None)
        if rpc_mark is None:
            continue
        table[rpc_mark.name] = (value, rpc_mark)
    return table


def get_rpc_mark(f, default=default_rpc_mark):
    """Gets an RPC mark from a method."""
    return getattr(f, RPC_MARK_ATTR, default)
