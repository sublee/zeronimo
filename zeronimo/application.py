# -*- coding: utf-8 -*-
"""
   zeronimo.application
   ~~~~~~~~~~~~~~~~~~~~

   A tools for application which a worker wraps.

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

# from collections import namedtuple
import functools
import inspect

from .helpers import FALSE_RETURNER


__all__ = ['DEFAULT_RPC_SPEC', 'rpc', 'rpc_table', 'get_rpc_spec']


RPC_SPEC_ATTR = '__zeronimo__'


# RPCSpec = namedtuple('RPCSpec', 'name pass_call manual_ack')
class RPCSpec(object):

    __slots__ = ('name', 'pass_call', 'manual_ack', 'reject_if')

    def __init__(self, name, pass_call=False, manual_ack=False,
                 reject_if=FALSE_RETURNER):
        self.name = name
        self.pass_call = pass_call
        self.manual_ack = manual_ack
        self.reject_if = reject_if


def reject_if_registrar(rpc_spec):
    def register_reject_if(reject_if):
        rpc_spec.reject_if = reject_if
        return reject_if
    return register_reject_if


def _spec_as_rpc(f, name=None, pass_call=False, manual_ack=False):
    if name is None:
        name = f.__name__
    rpc_spec = RPCSpec(name, pass_call, manual_ack)
    setattr(f, RPC_SPEC_ATTR, rpc_spec)
    f.reject_if = reject_if_registrar(rpc_spec)
    return f


#: The :class:`RPCSpec` with default values.
DEFAULT_RPC_SPEC = RPCSpec(*inspect.getargspec(_spec_as_rpc).defaults)


def rpc(f=None, **kwargs):
    """Spec a method as RPC."""
    if f is not None:
        if isinstance(f, basestring):
            if 'name' in kwargs:
                raise ValueError('name option duplicated')
            kwargs['name'] = f
        else:
            return rpc(**kwargs)(f)
    return functools.partial(_spec_as_rpc, **kwargs)


def rpc_table(app):
    """Collects methods which are speced as RPC."""
    table = {}
    for attr, value in inspect.getmembers(app):
        rpc_spec = get_rpc_spec(value, default=None)
        if rpc_spec is None:
            continue
        table[rpc_spec.name] = (value, rpc_spec)
    return table


def get_rpc_spec(f, default=DEFAULT_RPC_SPEC):
    """Gets an RPC spec from a method."""
    return getattr(f, RPC_SPEC_ATTR, default)
