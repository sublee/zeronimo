# -*- coding: utf-8 -*-
"""
    zeronimo.messaging
    ~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import
from collections import namedtuple
try:
    import cPickle as pickle
except ImportError:
    import pickle

import msgpack

from .helpers import make_repr


__all__ = ['ACK', 'DONE', 'ITER', 'ACCEPT', 'REJECT', 'RETURN', 'RAISE',
           'YIELD', 'BREAK', 'Call', 'Reply', 'send', 'recv']


# masks
ACK = 0b10000000
DONE = 0b01000000
ITER = 0b00100000
# methods
ACCEPT = ACK | 0b01
REJECT = ACK | 0b10
RETURN = DONE | 0b01
RAISE = DONE | 0b10
YIELD = ITER | 0b01
BREAK = ITER | 0b10


_Call = namedtuple('_Call', ['function_name', 'args', 'kwargs', 'call_id',
                             'collector_address'])
_Reply = namedtuple('_Reply', ['method', 'data', 'call_id', 'work_id'])


class Call(_Call):

    def __repr__(self):
        return make_repr(self, keywords=self._fields)


class Reply(_Reply):

    def __repr__(self):
        method = {
            ACCEPT: 'ACCEPT',
            REJECT: 'REJECT',
            RETURN: 'RETURN',
            RAISE: 'RAISE',
            YIELD: 'YIELD',
            BREAK: 'BREAK',
        }[self.method]
        class M(object):
            def __repr__(self):
                return method
        return make_repr(self, keywords=self._fields, data={'method': M()})


PICKLE_MAGIC_KEY = '__pickle__'


def default(obj):
    """The default serializer for MessagePack."""
    return {PICKLE_MAGIC_KEY: pickle.dumps(obj)}


def object_hook(obj):
    """The object hook for MessagePack."""
    if PICKLE_MAGIC_KEY in obj:
        return pickle.loads(obj[PICKLE_MAGIC_KEY])
    return obj


def send(socket, obj, flags=0, topic=None):
    """Sends a Python object via a ZeroMQ socket. It also can append PUB/SUB
    topic.
    """
    serial = msgpack.packb(obj, default=default)
    if topic:
        return socket.send_multipart([topic, serial], flags)
    else:
        return socket.send(serial, flags)


def recv(socket, flags=0):
    """Receives a Python object via a ZeroMQ socket."""
    serial = socket.recv_multipart(flags)[-1]
    try:
        return msgpack.unpackb(serial, object_hook=object_hook)
    except BaseException as exc:
        exc.serial = serial
        raise
