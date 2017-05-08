# -*- coding: utf-8 -*-
"""
   zeronimo.helpers
   ~~~~~~~~~~~~~~~~

   Helper functions.

   :copyright: (c) 2013-2017 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from errno import EINTR

import zmq


__all__ = ['FALSE_RETURNER', 'class_name', 'make_repr', 'socket_type_name',
           'repr_socket', 'eintr_retry', 'eintr_retry_zmq']


#: A function which always returns ``False``.  It is used for default of
#: `Worker.reject_if` and `Fanout.drop_if`.
FALSE_RETURNER = lambda *a, **k: False


def class_name(obj):
    """Returns the class name of the object."""
    return type(obj).__name__


def _repr_attr(obj, attr, data=None, reprs=None):
    val = getattr(obj, attr)
    if data is not None:
        val = data.get(attr, val)
    if reprs is None:
        repr_f = repr
    else:
        repr_f = reprs.get(attr, repr)
    return repr_f(val)


def make_repr(obj, params=None, keywords=None, data=None, name=None,
              reprs=None):
    """Generates a string of object initialization code style.  It is useful
    for custom __repr__ methods::

       class Example(object):

           def __init__(self, param, keyword=None):
               self.param = param
               self.keyword = keyword

           def __repr__(self):
               return make_repr(self, ['param'], ['keyword'])

    See the representation of example object::

       >>> Example('hello', keyword='world')
       Example('hello', keyword='world')
    """
    opts = []
    if params is not None:
        opts.append(', '.join(
            _repr_attr(obj, attr, data, reprs) for attr in params))
    if keywords is not None:
        opts.append(', '.join(
            '%s=%s' % (attr, _repr_attr(obj, attr, data, reprs))
            for attr in keywords))
    if name is None:
        name = class_name(obj)
    return '%s(%s)' % (name, ', '.join(opts))


_socket_type_names = {}
for name in ('PAIR PUB SUB REQ REP DEALER ROUTER PULL PUSH XPUB XSUB '
             'STREAM').split():
    try:
        socket_type = getattr(zmq, name)
    except AttributeError:
        continue
    assert socket_type not in _socket_type_names
    _socket_type_names[socket_type] = name


def socket_type_name(socket_type):
    """Gets the ZeroMQ socket type name."""
    return _socket_type_names[socket_type]


def repr_socket(socket):
    return '%s[%d]' % (socket_type_name(socket.type), socket.fd)


def eintr_retry(exc_type, f, *args, **kwargs):
    """Calls a function.  If an error of the given exception type with
    interrupted system call (EINTR) occurs calls the function again.
    """
    while True:
        try:
            return f(*args, **kwargs)
        except exc_type as exc:
            if exc.errno != EINTR:
                raise
        else:
            break


def eintr_retry_zmq(f, *args, **kwargs):
    """The specialization of :func:`eintr_retry` by :exc:`zmq.ZMQError`."""
    return eintr_retry(zmq.ZMQError, f, *args, **kwargs)
