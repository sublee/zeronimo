# -*- coding: utf-8 -*-
"""
   zeronimo.application
   ~~~~~~~~~~~~~~~~~~~~

   A tools for application which a worker wraps.

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import functools
import inspect

from zeronimo.helpers import FALSE_RETURNER


__all__ = ['NULL_RPC_SPEC', 'rpc', 'rpc_spec_table', 'get_rpc_spec']


RPC_SPEC_ATTR = '__zeronimo__'


class RPCSpec(object):

    __slots__ = ('name', 'pass_call', 'reject_if')

    def __init__(self, name=None, pass_call=False, reject_if=FALSE_RETURNER):
        self.name = name
        self.pass_call = pass_call
        self.reject_if = reject_if


def register_reject_if(rpc_spec, reject_if):
    rpc_spec.reject_if = reject_if
    return reject_if


#: The :class:`RPCSpec` with default values.
NULL_RPC_SPEC = RPCSpec()


def _rpc(f, name=None, pass_call=False, reject_if=None):
    if name is None:
        name = f.__name__
    rpc_spec = RPCSpec(name, pass_call)
    setattr(f, RPC_SPEC_ATTR, rpc_spec)
    f.reject_if = functools.partial(register_reject_if, rpc_spec)
    if reject_if is not None:
        f.reject_if(reject_if)
    return f


def rpc(f=None, **kwargs):
    """Marks a method as RPC."""
    if f is not None:
        if isinstance(f, basestring):
            if 'name' in kwargs:
                raise ValueError('name option duplicated')
            kwargs['name'] = f
        else:
            return rpc(**kwargs)(f)
    return functools.partial(_rpc, **kwargs)


def rpc_spec_table(app):
    """Collects methods which are speced as RPC."""
    table = {}
    for attr, value in inspect.getmembers(app):
        rpc_spec = get_rpc_spec(value, default=None)
        if rpc_spec is None:
            continue
        table[rpc_spec.name] = (value, rpc_spec)
    return table


def get_rpc_spec(f, default=NULL_RPC_SPEC):
    """Gets an RPC spec from a method."""
    return getattr(f, RPC_SPEC_ATTR, default)
