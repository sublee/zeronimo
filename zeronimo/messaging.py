# -*- coding: utf-8 -*-
"""
    zeronimo.messaging
    ~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import
from collections import namedtuple
try:
    import cPickle as pickle
except ImportError:
    import pickle

import zmq

from .helpers import make_repr


__all__ = ['ACK', 'DONE', 'ITER', 'ACCEPT', 'REJECT', 'RETURN', 'RAISE',
           'YIELD', 'BREAK', 'PACK', 'UNPACK', 'Call', 'Reply', 'send', 'recv']


# method masks
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


#: The default function to pack message.
PACK = pickle.dumps

#: The default function to unpack message.
UNPACK = pickle.loads


_Call = namedtuple('_Call', ['funcname', 'args', 'kwargs', 'call_id',
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


def send(socket, obj, flags=0, topic=None, pack=PACK):
    """Sends a Python object via a ZeroMQ socket. It also can append PUB/SUB
    topic.
    """
    if topic:
        socket.send(topic, flags | zmq.SNDMORE)
    message = pack(obj)
    return socket.send(message, flags)


def recv(socket, flags=0, unpack=UNPACK):
    """Receives a Python object via a ZeroMQ socket."""
    message = socket.recv_multipart(flags)[-1]
    try:
        return unpack(message)
    except BaseException as exc:
        exc.message = message
        raise
